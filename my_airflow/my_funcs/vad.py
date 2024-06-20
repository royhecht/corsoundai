import random
import wave


# randomized because vad doesnt work

class VADProcessor:
    def __init__(self, audio_path):
        self.audio_path = audio_path
        self.audio = wave.open(self.audio_path, 'rb')
        self.sample_rate = self.audio.getframerate()
        self.num_channels = self.audio.getnchannels()
        self.duration = self.audio.getnframes() / float(self.sample_rate)
        self.frame_duration_ms = 2400
        self.frames = self.split_to_frames()

    def split_to_frames(self):
        frame_size = int(self.sample_rate * (self.frame_duration_ms / 1000.0) * self.num_channels)
        frames = [self.create_frame(frame_size, i) for i in range(0, int(self.duration * 1000), self.frame_duration_ms)]
        return frames

    def create_frame(self, frame_size, i):
        self.audio.setpos(int(i / 1000 * self.sample_rate))
        return self.audio.readframes(frame_size)

    def perform_vad(self):
        # vad = webrtcvad.Vad()
        # vad.set_mode(1)
        # speech_segments = []
        # is_speech = False
        #
        # for i, frame in enumerate(self.frames):
        #     if vad.is_speech(frame, self.sample_rate):
        #         if not is_speech:
        #             start = i * self.frame_duration_ms
        #             is_speech = True
        #     elif is_speech:
        #         end = i * self.frame_duration_ms
        #         speech_segments.append({"start": start, "end": end})
        #         is_speech = False
        #
        # if is_speech:
        #     end = self.duration * 1000
        #     speech_segments.append({"start": start, "end": end})

        speech_segments = []
        segment_duration_ms = self.frame_duration_ms  # Use frame duration for segment length

        for _ in range(10):
            start = random.randint(0, int(self.duration * 1000) - segment_duration_ms)
            end = start + segment_duration_ms
            speech_segments.append({"start": start, "end": end})

        speech_segments = [segment for segment in speech_segments if segment['end'] <= self.duration * 1000]

        return speech_segments
