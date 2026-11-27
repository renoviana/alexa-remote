const http2 = require('http2');
const EventEmitter = require('events');
const { StringDecoder } = require('string_decoder');

// Boundary line of the multipart directives stream, e.g. "--------abcde123" (closing one ends with "--")
const STREAM_BOUNDARY = /(?:^|\r?\n)--[^\s]*\r?\n/;
const MAX_STREAM_BUFFER = 1024 * 1024;

class AlexaHttp2Push extends EventEmitter {

    constructor(options, update_access_token) {
        super();

        this._options = options;
        this.stop = false;
        this.client = null;
        this.stream = null;
        this.pingPongInterval = null;
        this.errorRetryCounter = 0;
        this.reconnectTimeout = null;
        this.pongTimeout = null;
        this.initTimeout = null;
        this.connectionActive = false;
        this.access_token = null;
        this.update_access_token = update_access_token;
        this.inClosing = false;
    }

    isConnected() {
        return this.connectionActive;
    }

    connect() {
        this.inClosing = false;
        this.update_access_token(token => {
            this.access_token = token;

            let host = 'bob-dispatch-prod-eu.amazon.com';
            if (this._options.pushDispatchHost) {
                host = this._options.pushDispatchHost;
            } else if (this._options.amazonPage === 'amazon.com') {
                host = 'bob-dispatch-prod-na.amazon.com';
            } else if (this._options.amazonPage === 'amazon.ca') {
                host = 'bob-dispatch-prod-na.amazon.com';
            } else if (this._options.amazonPage === 'amazon.com.mx') {
                host = 'bob-dispatch-prod-na.amazon.com';
            } else if (this._options.amazonPage === 'amazon.com.br') {
                host = 'bob-dispatch-prod-na.amazon.com';
            } else if (this._options.amazonPage === 'amazon.co.jp') {
                host = 'bob-dispatch-prod-fe.amazon.com';
            } else if (this._options.amazonPage === 'amazon.com.au') {
                host = 'bob-dispatch-prod-fe.amazon.com';
            } else if (this._options.amazonPage === 'amazon.com.in') {
                host = 'bob-dispatch-prod-fe.amazon.com';
            } else if (this._options.amazonPage === 'amazon.co.nz') {
                host = 'bob-dispatch-prod-fe.amazon.com';
            }
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Use host ${host}`);


            const http2Options = {
                ':method': 'GET',
                ':path': '/v20160207/directives',
                ':authority': host,
                ':scheme': 'https',
                'authorization': `Bearer ${this.access_token}`,
                'accept-encoding': 'gzip',
                'user-agent': 'okhttp/4.3.2-SNAPSHOT',
            };

            const onHttp2Close = (code, reason, immediateReconnect) => {
                if (this.inClosing) return;
                this.inClosing = true;
                if (reason) {
                    reason = reason.toString();
                }
                try {
                    this.stream && this.stream.destroy();
                } catch (err) {
                    // ignore
                }
                try {
                    this.client && this.client.destroy();
                } catch (err) {
                    // ignore
                }
                this.client = null;
                this.stream = null;
                this.connectionActive = false;
                this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Close: ${code}: ${reason}`);
                if (this.initTimeout) {
                    clearTimeout(this.initTimeout);
                    this.initTimeout = null;
                }
                if (this.pingPongInterval) {
                    clearInterval(this.pingPongInterval);
                    this.pingPongInterval = null;
                }
                if (this.pongTimeout) {
                    clearTimeout(this.pongTimeout);
                    this.pongTimeout = null;
                }
                if (this.stop) {
                    return;
                }
                if (this.errorRetryCounter > 100) {
                    this.emit('disconnect', false, 'Too many failed retries. Check cookie and data');
                    return;
                }

                this.errorRetryCounter++;

                const retryDelay = (immediateReconnect || this.errorRetryCounter === 1) ? 1 : Math.min(60, this.errorRetryCounter * 5);
                this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Retry Connection in ${retryDelay}s`);
                if (code !== undefined || reason !== undefined) {
                    this.emit('disconnect', true, `Retry Connection in ${retryDelay}s (${code}: ${reason})`);
                } else {
                    this.emit('disconnect', true, `Retry Connection in ${retryDelay}s`);
                }
                this.reconnectTimeout && clearTimeout(this.reconnectTimeout);
                this.reconnectTimeout = setTimeout(() => {
                    this.reconnectTimeout = null;
                    this.connect();
                }, retryDelay * 1000);
            };

            const onPingResponse = (resetErrorCount) => {
                if (this.initTimeout) {
                    clearTimeout(this.initTimeout);
                    this.initTimeout = null;
                    this._options.logger && this._options.logger('Alexa-Remote HTTP2-PUSH: Initialization completed');
                    this.emit('connect');
                }
                if (this.pongTimeout) {
                    clearTimeout(this.pongTimeout);
                    this.pongTimeout = null;
                }
                this.connectionActive = true;
                if (resetErrorCount) {
                    this.errorRetryCounter = 0;
                }
            };

            try {
                this.client = http2.connect(`https://${http2Options[':authority']}`,  () => {
                    if (!this.client) {
                        return;
                    }
                    try {
                        this.stream = this.client.request(http2Options);
                    } catch (error) {
                        this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Error on Request ${error.message}`);
                        this.emit('error', error);
                        return;
                    }

                    this.stream.on('response', async (headers) => {
                        if (headers[':status'] === 403) {
                            this._options.logger && this._options.logger('Alexa-Remote HTTP2-PUSH: Error 403 .... refresh token');
                            this.update_access_token(token => {
                                if (token) {
                                    this.access_token = token;
                                }
                                onHttp2Close(headers[':status'], undefined, this.errorRetryCounter < 3);
                            });
                        } else if (headers[':status'] !== 200) {
                            onHttp2Close(headers[':status']);
                        }
                    });

                    const decoder = new StringDecoder('utf8'); // keeps multi-byte characters split across chunks intact
                    this.streamBuffer = '';
                    this.stream.on('data', (chunk) => {
                        if (this.stop) {
                            this.stream && this.stream.end();
                            this.client && this.client.close();
                            return;
                        }
                        this.handleStreamData(decoder.write(chunk), () => {
                            if (this.pingPongInterval) {
                                return;
                            }
                            this.client.ping(() => onPingResponse(false));

                            this.pingPongInterval = setInterval(() => {
                                if (!this.stream || !this.client) {
                                    return;
                                }
                                this._options.logger && this._options.logger('Alexa-Remote HTTP2-PUSH: Send Ping');
                                // console.log('SEND: ' + msg.toString('hex'));
                                try {
                                    this.client.ping(() => onPingResponse(true));
                                } catch (error) {
                                    this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Error on Ping ${error.message}`);
                                }

                                this.pongTimeout = setTimeout(() => {
                                    this.pongTimeout = null;
                                    this._options.logger && this._options.logger('Alexa-Remote HTTP2-PUSH: No Pong received after 30s');
                                    this.stream && this.stream.end();
                                    this.client && this.client.close();
                                    this.connectionActive = false;
                                }, 30000);
                            }, 180000);
                        });
                    });

                    this.stream.on('close', onHttp2Close);

                    this.stream.on('error', (error) => {
                        this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Stream-Error: ${error}`);
                        this.emit('error', error);
                        this.stream && this.stream.end();
                        this.client && this.client.close();
                    });
                });

                this.client.on('close', onHttp2Close);

                this.client.on('error', (error) => {
                    this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Client-Error: ${error}`);
                    this.emit('error', error);
                    this.stream && this.stream.end();
                    this.client && this.client.close();
                });
            }
            catch (err) {
                this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Error on Init ${err.message}`);
                this._options.logger && this._options.logger(err.stack);
                this.emit('error', err);
                return;
            }
            this.initTimeout && clearTimeout(this.initTimeout);

            this.initTimeout = setTimeout(() => {
                this._options.logger && this._options.logger('Alexa-Remote HTTP2-PUSH: Initialization not done within 30s');
                try {
                    this.stream && this.stream.end();
                    this.client && this.client.close();
                } catch (err) {
                    // just make sure
                }
                if (this.stream || !this.reconnectTimeout) { // it seems no close was emitted so far?!
                    onHttp2Close();
                }
            }, 30000);
        });
    }

    /**
     * The directives stream is multipart: parts separated by boundary lines like "--------abcde123".
     * HTTP/2 data chunks do not match parts, a big message can span several chunks,
     * so data is buffered and only complete parts (followed by a boundary line) are handled.
     */
    handleStreamData(data, onBoundary) {
        this.streamBuffer = (this.streamBuffer || '') + data;
        let match;
        while ((match = STREAM_BOUNDARY.exec(this.streamBuffer))) {
            const part = this.streamBuffer.substring(0, match.index);
            this.streamBuffer = this.streamBuffer.substring(match.index + match[0].length);
            onBoundary && onBoundary();
            this.handleStreamPart(part);
        }
        if (this.streamBuffer.length > MAX_STREAM_BUFFER) {
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: No boundary within ${MAX_STREAM_BUFFER} characters, dropping data`);
            this.emit('unexpected-response', `No boundary within ${MAX_STREAM_BUFFER} characters: ${this.streamBuffer.substring(0, 500)}`);
            this.streamBuffer = '';
        }
    }

    handleStreamPart(part) {
        part = part.trim();
        if (!part) {
            return;
        }
        if (!part.startsWith('Content-Type: application/json')) {
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Unexpected part: ${part}`);
            this.emit('unexpected-response', `Unexpected part: ${part}`);
            return;
        }
        const json_start = part.indexOf('{');
        const json_end = part.lastIndexOf('}');
        if (json_start === -1 || json_end === -1) {
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Unexpected Response, could not find json in part: ${part}`);
            this.emit('unexpected-response', `Could not find json in part: ${part}`);
            return;
        }
        const message = part.substring(json_start, json_end + 1);
        let data;
        try {
            data = JSON.parse(message);
        } catch (err) {
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Could not parse json: ${message}: ${err.message}`);
            this.emit('unexpected-response', `Could not parse json: ${message}: ${err.message}`);
            return;
        }
        if (!data || !data.directive || !data.directive.payload || !Array.isArray(data.directive.payload.renderingUpdates)) {
            // Valid JSON but not the known format: hand it out so new message types can be discovered
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Unknown message without renderingUpdates: ${message}`);
            this.emit('unknown', data);
            return;
        }
        // Each update is handled on its own, so one broken update does not drop the others
        data.directive.payload.renderingUpdates.forEach(update => this.handleRenderingUpdate(update));
    }

    handleRenderingUpdate(update) {
        let command;
        let payload;
        try {
            if (!update || !update.resourceMetadata) {
                throw new Error('missing resourceMetadata');
            }
            const dataContent = typeof update.resourceMetadata === 'string' ? JSON.parse(update.resourceMetadata) : update.resourceMetadata;
            if (!dataContent || !dataContent.command) {
                throw new Error('missing command');
            }
            command = dataContent.command;
            payload = typeof dataContent.payload === 'string' ? JSON.parse(dataContent.payload) : dataContent.payload;
        } catch (err) {
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Could not parse renderingUpdate (${err.message}): ${JSON.stringify(update)}`);
            this.emit('unknown', update);
            return;
        }

        this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Command ${command}: ${JSON.stringify(payload, null, 4)}`);
        try {
            this.emit('command', command, payload);
        } catch (err) {
            // An exception in a listener must not end up as an uncaught exception in the stream handler
            this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Error while handling command ${command}: ${err.message}`);
            this.emit('unexpected-response', `Error while handling command ${command}: ${err.message}`);
        }
    }

    disconnect() {
        if (this.reconnectTimeout) {
            clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = null;
        }
        if (this.pollingTimeout) {
            clearTimeout(this.pollingTimeout);
            this.pollingTimeout = null;
        }
        if (this.initTimeout) {
            clearTimeout(this.initTimeout);
            this.initTimeout = null;
        }
        this.stop = true;
        if (!this.client && !this.stream) {
            return;
        }
        try {
            this.stream && this.stream.end();
            this.client && this.client.close();
        } catch (e) {
            this.connectionActive && this._options.logger && this._options.logger(`Alexa-Remote HTTP2-PUSH: Disconnect error: ${e.message}`);
        }
    }
}

module.exports = AlexaHttp2Push;
