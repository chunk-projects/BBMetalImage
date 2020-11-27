//
//  BBMetalVideoWriter.swift
//  BBMetalImage
//
//  Created by Kaibo Lu on 4/30/19.
//  Copyright © 2019 Kaibo Lu. All rights reserved.
//

import AVFoundation

public typealias BBMetalVideoWriterStart = (CMTime) -> Void

public typealias BBMetalVideoWriterProgress = (BBMetalVideoWriterProgressType) -> Void

public enum BBMetalVideoWriterProgressType {
    case video(CMTime, Bool)
    case audio(CMTime, Bool)
}

public enum BBMetalPauseState {
    case playing
    case pauseRequested
    case paused
}

/// Video writer writing video file
public class BBMetalVideoWriter {
        
    /// Pause/resume functionality
    private var pauseState: BBMetalPauseState = .playing
    private var awaitingTimeOffset = false
    private var timeOffset = CMTime.zero
    private var lastVideoTime = CMTime.zero
    private var lastAudioTime = CMTime.zero
    
    public var isPaused: Bool { return pauseState == .paused }
    public var isPlaying: Bool { return pauseState == .playing }
    
    /// URL of video file
    public let url: URL
    /// Video frame size
    public let frameSize: BBMetalIntSize
    /// Video file type
    public let fileType: AVFileType
    /// Video settings
    public let outputSettings: [String : Any]
    
    private var computePipeline: MTLComputePipelineState!
    private var outputTexture: MTLTexture!
    private let threadgroupSize: MTLSize
    private var threadgroupCount: MTLSize
    
    private var writer: AVAssetWriter!
    private var videoInput: AVAssetWriterInput!
    private var videoPixelBufferInput: AVAssetWriterInputPixelBufferAdaptor!
    private var videoPixelBuffer: CVPixelBuffer!
    
    /// Whether the video contains audio track (true by default)
    public var hasAudioTrack: Bool {
        get {
            lock.wait()
            let h = _hasAudioTrack
            lock.signal()
            return h
        }
        set {
            lock.wait()
            _hasAudioTrack = newValue
            lock.signal()
        }
    }
    private var _hasAudioTrack: Bool
    
    /// A Boolean value (true by defaut) that indicates whether the input should tailor its processing of media data for real-time sources 
    public var expectsMediaDataInRealTime: Bool {
        get {
            lock.wait()
            let e = _expectsMediaDataInRealTime
            lock.signal()
            return e
        }
        set {
            lock.wait()
            _expectsMediaDataInRealTime = newValue
            lock.signal()
        }
    }
    private var _expectsMediaDataInRealTime: Bool
    
    private var audioInput: AVAssetWriterInput!
    
    private var progress: BBMetalVideoWriterProgress?
    
    private var startHandler: BBMetalVideoWriterStart?
    
    private let lock: DispatchSemaphore
    
    deinit {
        lock.wait()
        NotificationCenter.default.removeObserver(self)
        lock.signal()
    }
    
    public init(
        url: URL,
        frameSize: BBMetalIntSize,
        fileType: AVFileType = .mp4,
        outputSettings: [String : Any] = [AVVideoCodecKey : AVVideoCodecH264]
    ) { 
        self.url = url
        self.frameSize = frameSize
        self.fileType = fileType
        self.outputSettings = outputSettings
        
        let library = try! BBMetalDevice.sharedDevice.makeDefaultLibrary(bundle: Bundle(for: BBMetalVideoWriter.self))
        let kernelFunction = library.makeFunction(name: "passThroughKernel")!
        computePipeline = try! BBMetalDevice.sharedDevice.makeComputePipelineState(function: kernelFunction)
        
        let descriptor = MTLTextureDescriptor()
        descriptor.pixelFormat = .bgra8Unorm
        descriptor.width = frameSize.width
        descriptor.height = frameSize.height
        descriptor.usage = [.shaderRead, .shaderWrite]
        outputTexture = BBMetalDevice.sharedDevice.makeTexture(descriptor: descriptor)
        
        threadgroupSize = MTLSize(width: 16, height: 16, depth: 1)
        threadgroupCount = MTLSize(width: (frameSize.width + threadgroupSize.width - 1) / threadgroupSize.width,
                                   height: (frameSize.height + threadgroupSize.height - 1) / threadgroupSize.height,
                                   depth: 1)
        
        _hasAudioTrack = true
        _expectsMediaDataInRealTime = true
        lock = DispatchSemaphore(value: 1)
    }
    
    /// Starts receiving Metal texture and writing video file
    /// - Parameters:
    ///   - startHandler: a closure to call after starting writting
    ///   - progress: a closure to call after writting a video frame or an audio buffer
    public func start(startHandler: BBMetalVideoWriterStart? = nil, progress: BBMetalVideoWriterProgress? = nil) {
        lock.wait()
        defer { lock.signal() }
        
        self.startHandler = startHandler
        self.progress = progress
        pauseState = .playing
        timeOffset = CMTime.zero
        
        if writer == nil {
            if !prepareAssetWriter() {
                reset()
                return
            }
        } else {
            print("Should not call \(#function) before last writing operation is finished")
            return
        }
        if !writer.startWriting() {
            reset()
            print("Asset writer can not start writing")
        }
    }
    
    /// Finishes writing video file
    ///
    /// - Parameter completion: a closure to call after writing video file
    public func finish(completion: (() -> Void)?) {
        lock.wait()
        defer { lock.signal() }
        if let videoInput = self.videoInput,
            let writer = self.writer,
            writer.status == .writing {
            videoInput.markAsFinished()
            if let audioInput = self.audioInput {
                audioInput.markAsFinished()
            }
            let name = "com.Kaibo.BBMetalImage.VideoWriter.Finish"
            let object = NSObject()
            NotificationCenter.default.addObserver(self, selector: #selector(finishWritingNotification(_:)), name: NSNotification.Name(name), object: object)
            writer.finishWriting {
                // The comment code below leads to memory leak even using [weak self].
                // Using [unowned self] solves the memory leak, but not safe.
                // So use notification here.
                /*
                [weak self] in
                guard let self = self else { return }
                self.lock.wait()
                self.reset()
                self.lock.signal()
                */
                NotificationCenter.default.post(name: NSNotification.Name(name), object: object, userInfo: nil)
                completion?()
            }
        } else {
            print("Should not call \(#function) while video writer is not writing")
        }
    }
    
    /// Cancels writing video file
    public func cancel() {
        lock.wait()
        defer { lock.signal() }
        if let videoInput = self.videoInput,
            let writer = self.writer,
            writer.status == .writing {
            videoInput.markAsFinished()
            if let audioInput = self.audioInput {
                audioInput.markAsFinished()
            }
            writer.cancelWriting()
            reset()
        } else {
            print("Should not call \(#function) while video writer is not writing")
        }
    }
    
    private func prepareAssetWriter() -> Bool {
        writer = try? AVAssetWriter(url: url, fileType: fileType)
        if writer == nil {
            print("Can not create asset writer")
            return false
        }
        
        var settings = outputSettings
        settings[AVVideoWidthKey] = frameSize.width
        settings[AVVideoHeightKey] = frameSize.height
        videoInput = AVAssetWriterInput(mediaType: .video, outputSettings: settings)
        videoInput.expectsMediaDataInRealTime = _expectsMediaDataInRealTime
        if !writer.canAdd(videoInput) {
            print("Asset writer can not add video input")
            return false
        }
        writer.add(videoInput)
        
        let attributes: [String : Any] = [kCVPixelBufferPixelFormatTypeKey as String : kCVPixelFormatType_32BGRA,
                                          kCVPixelBufferWidthKey as String : frameSize.width,
                                          kCVPixelBufferHeightKey as String : frameSize.height]
        videoPixelBufferInput = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: videoInput, sourcePixelBufferAttributes: attributes)
        
        if _hasAudioTrack {
            let settings: [String : Any] = [AVFormatIDKey : kAudioFormatMPEG4AAC,
                                            AVNumberOfChannelsKey : 1,
                                            AVSampleRateKey : AVAudioSession.sharedInstance().sampleRate]
            audioInput = AVAssetWriterInput(mediaType: .audio, outputSettings: settings)
            audioInput.expectsMediaDataInRealTime = _expectsMediaDataInRealTime
            if !writer.canAdd(audioInput) {
                print("Asset writer can not add audio input")
                return false
            }
            writer.add(audioInput)
        }
        return true
    }
    
    private func reset() {
        writer = nil
        videoInput = nil
        videoPixelBufferInput = nil
        videoPixelBuffer = nil
        audioInput = nil
        startHandler = nil
        progress = nil
    }
    
    @objc private func finishWritingNotification(_ notification: Notification) {
        lock.wait()
        reset()
        NotificationCenter.default.removeObserver(self, name: notification.name, object: notification.object)
        lock.signal()
    }
}

extension BBMetalVideoWriter: BBMetalImageConsumer {
    public func add(source: BBMetalImageSource) {}
    public func remove(source: BBMetalImageSource) {}
    
    public func newTextureAvailable(_ texture: BBMetalTexture, from source: BBMetalImageSource) {
        if pauseState == .paused {
            return
        }
        lock.wait()
        
        let startHandler = self.startHandler
        let progress = self.progress
        var result: Bool?
        
        defer {
            lock.signal()
            
            if let startHandler = startHandler,
                let sampleTime = texture.sampleTime {
                startHandler(sampleTime)
            }
            
            if let progress = progress,
                let result = result,
                let sampleTime = texture.sampleTime {
                progress(.video(sampleTime, result))
            }
        }
        
        // Check nil
        guard let videoInput = self.videoInput else {
            return
        }
        guard let videoPixelBufferInput = self.videoPixelBufferInput else {
            return
        }
        guard let writer = self.writer else {
            return
        }
        guard let defaultTexture = texture as? BBMetalDefaultTexture else {
            print("No defaultTexture")
            return
        }
        guard let originalSampleBuffer = defaultTexture.sampleBuffer else {
            print("No originalSampleBuffer")
            return
        }
        let finalSampleBuffer = updateSampleBufferAndStoreTime(originalSampleBuffer, video: true)
        let finalSampleTime = CMSampleBufferGetPresentationTimeStamp(finalSampleBuffer)
        if videoPixelBuffer == nil {
            // First frame
            self.startHandler = nil // Set start handler to nil to ensure it is called only once
            writer.startSession(atSourceTime: finalSampleTime)
            guard let pool = videoPixelBufferInput.pixelBufferPool,
                CVPixelBufferPoolCreatePixelBuffer(nil, pool, &videoPixelBuffer) == kCVReturnSuccess else {
                    print("Can not create pixel buffer")
                    return
            }
        }
        
        // Render to output texture
        guard let commandBuffer = BBMetalDevice.sharedCommandQueue.makeCommandBuffer(),
            let encoder = commandBuffer.makeComputeCommandEncoder() else {
                CVPixelBufferUnlockBaseAddress(videoPixelBuffer, [])
                print("Can not create compute command buffer or encoder")
                return
        }
        
        encoder.setComputePipelineState(computePipeline)
        encoder.setTexture(outputTexture, index: 0)
        encoder.setTexture(texture.metalTexture, index: 1)
        encoder.dispatchThreadgroups(threadgroupCount, threadsPerThreadgroup: threadgroupSize)
        encoder.endEncoding()
        
        commandBuffer.commit()
        commandBuffer.waitUntilCompleted() // Wait to make sure that output texture contains new data
        
        // Check status
        guard videoInput.isReadyForMoreMediaData,
            writer.status == .writing else {
                print("Asset writer or video input is not ready for writing this frame")
                return
        }
        
        // Copy data from metal texture to pixel buffer
        guard videoPixelBuffer != nil,
            CVPixelBufferLockBaseAddress(videoPixelBuffer, []) == kCVReturnSuccess else {
                print("Pixel buffer can not lock base address")
                return
        }
        guard let baseAddress = CVPixelBufferGetBaseAddress(videoPixelBuffer) else {
            CVPixelBufferUnlockBaseAddress(videoPixelBuffer, [])
            print("Can not get pixel buffer base address")
            return
        }
        
        let bytesPerRow = CVPixelBufferGetBytesPerRow(videoPixelBuffer)
        let region = MTLRegionMake2D(0, 0, outputTexture.width, outputTexture.height)
        outputTexture.getBytes(baseAddress, bytesPerRow: bytesPerRow, from: region, mipmapLevel: 0)
        
        result = videoPixelBufferInput.append(videoPixelBuffer, withPresentationTime: finalSampleTime)
        
        CVPixelBufferUnlockBaseAddress(videoPixelBuffer, [])
    }
}

extension BBMetalVideoWriter: BBMetalAudioConsumer {
    public func newAudioSampleBufferAvailable(_ sampleBuffer: CMSampleBuffer) {
        if pauseState == .paused {
            return
        }
        lock.wait()
        
        let progress = self.progress
        var result: Bool?
        
        defer {
            lock.signal()
            
            if let result = result,
                let progress = progress {
                progress(.audio(CMSampleBufferGetOutputPresentationTimeStamp(sampleBuffer), result))
            }
        }
        
        // Check nil
        guard let audioInput = self.audioInput,
            let writer = self.writer else { return }
        
        // Check first frame
        guard videoPixelBuffer != nil else { return }
        
        // Check status
        guard audioInput.isReadyForMoreMediaData,
            writer.status == .writing else {
                print("Asset writer or audio input is not ready for writing this frame")
                return
        }
        
        if pauseState == .pauseRequested {
            pauseState = .paused
            awaitingTimeOffset = true
        }
        if pauseState == .playing && awaitingTimeOffset {
            handleTimeOffset(sampleBuffer)
        }
        let finalSampleBuffer = updateSampleBufferAndStoreTime(sampleBuffer, video: false)
        result = audioInput.append(finalSampleBuffer)
    }
}

extension BBMetalVideoWriter {
    
    func getTimeAdjustedAudioBuffer(_ originalBuffer: CMSampleBuffer, offset: CMTime) -> CMSampleBuffer {
        var timeAdjustedBuffer: CMSampleBuffer?
        var originalTiming: CMSampleTimingInfo = CMSampleTimingInfo()
        CMSampleBufferGetSampleTimingInfo(originalBuffer, at: 0, timingInfoOut: &originalTiming)
        var timingInfo = CMSampleTimingInfo()
        timingInfo.presentationTimeStamp = CMTimeAdd(timingInfo.presentationTimeStamp, offset)
        timingInfo.decodeTimeStamp = CMTime.invalid
        CMSampleBufferCreateCopyWithNewTiming(allocator: kCFAllocatorDefault, sampleBuffer: originalBuffer, sampleTimingEntryCount: 1, sampleTimingArray: &timingInfo, sampleBufferOut: &timeAdjustedBuffer)
        return timeAdjustedBuffer!
    }
    
    func getTimeAdjustedVideoBuffer(_ originalBuffer: CMSampleBuffer, offset: CMTime) -> CMSampleBuffer {
        var count: CMItemCount = 0
        CMSampleBufferGetSampleTimingInfoArray(originalBuffer, entryCount: 0, arrayToFill: nil, entriesNeededOut: &count);
        var info = [CMSampleTimingInfo](repeating: CMSampleTimingInfo(duration: CMTimeMake(value: 0, timescale: 0), presentationTimeStamp: CMTimeMake(value: 0, timescale: 0), decodeTimeStamp: CMTimeMake(value: 0, timescale: 0)), count: count)
        CMSampleBufferGetSampleTimingInfoArray(originalBuffer, entryCount: count, arrayToFill: &info, entriesNeededOut: &count);
        for i in 0..<count {
            info[i].decodeTimeStamp = CMTimeSubtract(info[i].decodeTimeStamp, offset)
            info[i].presentationTimeStamp = CMTimeSubtract(info[i].presentationTimeStamp, offset)
        }
        var timeAdjustedBuffer: CMSampleBuffer?
        CMSampleBufferCreateCopyWithNewTiming(allocator: kCFAllocatorDefault, sampleBuffer: originalBuffer, sampleTimingEntryCount: count, sampleTimingArray: &info, sampleBufferOut: &timeAdjustedBuffer);
        return timeAdjustedBuffer!
    }
    
    func updateSampleBufferAndStoreTime(_ inputSampleBuffer: CMSampleBuffer, video: Bool) -> CMSampleBuffer {
        var sampleBuffer = inputSampleBuffer
        if timeOffset.value > 0 {
            if video {
                sampleBuffer = getTimeAdjustedVideoBuffer(inputSampleBuffer, offset: timeOffset)
            } else {
                sampleBuffer = getTimeAdjustedAudioBuffer(inputSampleBuffer, offset: timeOffset)
            }
        }
        // record most recent time so we know the length of the pause
        var presentationTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        let duration = CMSampleBufferGetDuration(sampleBuffer)
        if duration.value > 0 {
            presentationTime = CMTimeAdd(presentationTime, duration)
        }
        if video {
            lastVideoTime = presentationTime
        } else {
            lastAudioTime = presentationTime
        }
        return sampleBuffer
    }
    
    func cmTimeIsValid(_ cmTime: CMTime) -> Bool {
        return cmTime.flags.contains(.valid)
    }
    
    func handleTimeOffset(_ sampleBuffer: CMSampleBuffer) {
        awaitingTimeOffset = false
        var pts: CMTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        let last: CMTime = lastAudioTime
        if cmTimeIsValid(last) {
            if cmTimeIsValid(timeOffset) {
                pts = CMTimeSubtract(pts, timeOffset)
            }
            let offset: CMTime = CMTimeSubtract(pts, last)
            print("Adding offset \(CMTimeGetSeconds(offset))")
            // this stops us having to set a scale for timeOffset before we see the first video time
            if timeOffset.value == 0 {
                timeOffset = offset
                print("Setting time offset (first time): \(CMTimeGetSeconds(timeOffset))")
            } else {
                timeOffset = CMTimeAdd(timeOffset, offset);
                print("Setting time offset: \(CMTimeGetSeconds(timeOffset))")
            }
        }
        lastVideoTime.flags = CMTimeFlags()
        lastAudioTime.flags = CMTimeFlags()
    }
    
    public func pause() {
        if pauseState == .playing {
            pauseState = .pauseRequested
        }
    }
    
    public func resume() {
        if pauseState == .paused {
            pauseState = .playing
        }
    }
    
}
