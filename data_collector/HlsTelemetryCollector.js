/*
Data Collector

A telemetry collector for HLS.js that captures the viewer’s current video quality watched and 
beginning and end of a buffering state of the test page short video demo, then sends this data 
both to the browser console and to a FastAPI backend endpoint.

 */

class HlsTelemetryCollector {
  constructor(hls, video, config = {}) {
    if (!hls) throw new Error("HlsTelemetryCollector: hls instance is required");
    if (!video) throw new Error("HlsTelemetryCollector: video element is required");

    this.hls = hls;
    this.video = video;

    // Metadata
    this.sessionId = config.sessionId || crypto.randomUUID();
    this.viewerId = config.viewerId || "anonymous";
    this.eventId = config.eventId || "default-event";

    // FastAPI backend endpoint
    this.endpointUrl = config.endpointUrl || null;

    // Internal state
    this.bufferStart = null;

    // Attach event listeners
    this._attachListeners();
  }

  /*
   Send telemetry event to the:
   - console and
   - POSTs to the backend
   */
  _send(event) {
    const payload = {
      ...event,
      sessionId: this.sessionId,
      viewerId: this.viewerId,
      eventId: this.eventId,
      timestamp: Date.now(),
    };

    console.log("📡 Telemetry:", payload);

    if (this.endpointUrl) {
      fetch(this.endpointUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }).catch((err) => {
        console.warn("Telemetry send failed:", err);
      });
    }
  }

  /*
   Attach all HLS.js event listeners
   */
  _attachListeners() {
    // QUALITY CHANGE
    this.hls.on(Hls.Events.LEVEL_SWITCHED, (_, data) => {
      const level = this.hls.levels[data.level];

      this._send({
        event_type: "quality_change",
        playback_position: this.video.currentTime,
        quality: {
          level_index: data.level,
          height: level.height,
          bitrate: level.bitrate,
        },
      });
    });

    // BUFFERING START
    this.hls.on(Hls.Events.BUFFER_STALLED, () => {
      this.bufferStart = Date.now();

      this._send({
        event_type: "buffering_start",
        playback_position: this.video.currentTime,
      });
    });

     // BUFFERING END    
    this.hls.on(Hls.Events.BUFFER_APPENDED, () => {
      if (!this.bufferStart) return;

      const end = Date.now();

      this._send({
        event_type: "buffering_end",
        playback_position: this.video.currentTime,
        buffering: {
          started_at: this.bufferStart,
          ended_at: end,
          duration_ms: end - this.bufferStart,
        },
      });

      this.bufferStart = null;
    });
  }
}

// Export for module environments
if (typeof module !== "undefined") {
  module.exports = HlsTelemetryCollector;
}
