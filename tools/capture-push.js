#!/usr/bin/env node
/*
 * Captures every push message Amazon sends, to find and document events that are not mapped yet.
 *
 * Usage:
 *   node tools/capture-push.js [--page amazon.com.br] [--locale pt-BR] [--out capture]
 *                              [--proxy-ip localhost] [--proxy-port 3001] [--minutes 0]
 *
 * First run: open the printed proxy URL in a browser (one WITHOUT the Alexa app) and log in.
 * The login data is stored in <out>/auth.json and reused on the next runs.
 * Then use your Echo devices normally; stop with Ctrl+C (or let --minutes expire).
 *
 * Output in <out>/ (git-ignored):
 *   auth.json               login data - SECRET, never share it
 *   events-<timestamp>.jsonl every received message, raw (contains IDs and serial numbers)
 *   summary.json            one entry per command: count, status, payload keys and a REDACTED sample,
 *                           meant to be shared in issues/PRs
 */
'use strict';

const fs = require('fs');
const path = require('path');
const Alexa = require('../alexa-remote');

// Commands alexa-remote receives but intentionally drops (see initPushConnection)
const IGNORED_COMMANDS = ['PUSH_MICROPHONE_STATE', 'PUSH_DELETE_DOPPLER_ACTIVITIES', 'PUSH_DEVICE_SETUP_STATE_CHANGE', 'NotifyMediaSessionsUpdated'];
const SENSITIVE_KEY = /(customerid|userid|personid|commsid|accountid|serialnumber|serial|email|token|cookie|phone|address|utteranceid|entryid|^id$)/i;

function parseArgs(argv) {
    const args = { page: 'amazon.com.br', locale: 'pt-BR', out: 'capture', proxyIp: 'localhost', proxyPort: 3001, minutes: 0 };
    const names = { '--page': 'page', '--locale': 'locale', '--out': 'out', '--proxy-ip': 'proxyIp', '--proxy-port': 'proxyPort', '--minutes': 'minutes' };
    for (let i = 0; i < argv.length; i++) {
        const name = names[argv[i]];
        if (!name || argv[i + 1] === undefined) {
            console.error(`Unknown or incomplete argument: ${argv[i]}`);
            process.exit(1);
        }
        args[name] = argv[++i];
    }
    args.proxyPort = Number(args.proxyPort);
    args.minutes = Number(args.minutes);
    return args;
}

function redact(value, key) {
    if (Array.isArray(value)) return value.map(v => redact(v, key));
    if (value && typeof value === 'object') {
        const result = {};
        for (const k of Object.keys(value)) result[k] = redact(value[k], k);
        return result;
    }
    if (typeof value === 'string') {
        // Nested JSON strings are common in Amazon payloads
        if (/^\s*[{[]/.test(value)) {
            try {
                return JSON.stringify(redact(JSON.parse(value), key));
            } catch (e) {
                // not JSON, fall through
            }
        }
        if (key && SENSITIVE_KEY.test(key)) return '<redacted>';
    }
    return value;
}

function keyPaths(value, prefix = '') {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return [];
    return Object.keys(value).flatMap(k => [`${prefix}${k}`, ...keyPaths(value[k], `${prefix}${k}.`)]);
}

const args = parseArgs(process.argv.slice(2));
const outDir = path.resolve(args.out);
fs.mkdirSync(outDir, { recursive: true });
const authFile = path.join(outDir, 'auth.json');
const summaryFile = path.join(outDir, 'summary.json');
const eventsFile = path.join(outDir, `events-${new Date().toISOString().replace(/[:.]/g, '-')}.jsonl`);
const events = fs.createWriteStream(eventsFile, { flags: 'a' });

const summary = fs.existsSync(summaryFile) ? JSON.parse(fs.readFileSync(summaryFile, 'utf8')) : {};

function record(type, name, payload) {
    const now = new Date().toISOString();
    events.write(`${JSON.stringify({ time: now, type, name, payload })}\n`);

    const id = `${type}:${name}`;
    const entry = summary[id] = summary[id] || { type, name, status: null, count: 0, firstSeen: now, keys: [], sample: null };
    entry.count++;
    entry.lastSeen = now;
    entry.keys = [...new Set([...entry.keys, ...keyPaths(payload)])].sort();
    if (!entry.sample) entry.sample = redact(payload);
    if (type === 'command' && !entry.status) entry.status = IGNORED_COMMANDS.includes(name) ? 'ignored' : 'mapped';
    return entry;
}

function writeSummary() {
    fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2));
}

function log(msg) {
    console.log(`[${new Date().toLocaleTimeString()}] ${msg}`);
}

const alexa = new Alexa();
let auth = fs.existsSync(authFile) ? JSON.parse(fs.readFileSync(authFile, 'utf8')) : null;
let started = false;

alexa.on('cookie', () => {
    if (!alexa.cookieData) return;
    auth = alexa.cookieData;
    fs.writeFileSync(authFile, JSON.stringify(auth, null, 2));
    log(`Login data saved to ${authFile}`);
});

alexa.on('command', ({ command, payload }) => {
    const entry = record('command', command, payload);
    log(`command ${command} (#${entry.count}${entry.status === 'ignored' ? ', ignored by alexa-remote' : ''})`);
});
alexa.on('ws-unknown-command', (command) => {
    // emitted right after 'command' for commands no case handles
    summary[`command:${command}`].status = 'unmapped';
    log(`  -> NOT MAPPED: ${command}`);
});
alexa.on('ws-unknown-message', (msg) => {
    record('unknown-message', 'unknown-message', msg);
    log('unknown message (not in renderingUpdates format)');
});
alexa.on('ws-unexpected-response', (msg) => {
    record('unexpected-response', 'unexpected-response', msg);
    log(`unexpected response: ${String(msg).slice(0, 200)}`);
});
alexa.on('ws-connect', () => log('push connected - use your devices now'));
alexa.on('ws-disconnect', (retry, msg) => log(`push disconnected (retry: ${retry}): ${msg}`));
alexa.on('ws-error', (err) => log(`push error: ${err && err.message ? err.message : err}`));

alexa.init({
    cookie: auth || undefined,
    formerRegistrationData: auth || undefined,
    macDms: auth ? auth.macDms : undefined,
    amazonPage: args.page,
    acceptLanguage: args.locale,
    amazonPageProxyLanguage: args.locale.replace('-', '_'),
    proxyOnly: true,
    setupProxy: true,
    proxyOwnIp: args.proxyIp,
    proxyPort: args.proxyPort,
    proxyLogLevel: 'warn',
    usePushConnection: true,
    autoQueryActivityOnTrigger: false,
    logger: process.env.DEBUG ? console.log : undefined,
}, (err) => {
    if (err) {
        // While waiting for the proxy login, init reports the URL as "error" and calls back again after login
        if (/Please open/.test(err.message || err)) return log(String(err.message || err));
        log(`Init failed: ${err.message || err}`);
        return process.exit(1);
    }
    if (started) return; // init calls back again after each cookie refresh
    started = true;
    const devices = Object.values(alexa.serialNumbers);
    log(`Logged in, ${devices.length} devices: ${devices.map(d => d.accountName).join(', ')}`);
    log(`Raw events: ${eventsFile}`);
    if (args.minutes > 0) setTimeout(stop, args.minutes * 60 * 1000);
});

const summaryInterval = setInterval(writeSummary, 30 * 1000);

function stop() {
    clearInterval(summaryInterval);
    writeSummary();
    events.end();
    const entries = Object.values(summary).sort((a, b) => b.count - a.count);
    console.log('\nSummary (all runs):');
    for (const e of entries) console.log(`  ${String(e.count).padStart(5)}  ${(e.status || e.type).padEnd(19)} ${e.name}`);
    console.log(`\nShareable, redacted summary: ${summaryFile}`);
    alexa.stop();
    process.exit(0);
}
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
