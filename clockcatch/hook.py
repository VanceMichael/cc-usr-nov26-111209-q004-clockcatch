import json
import urllib.parse


def handle(server, path, headers):
    parsed = urllib.parse.urlsplit(path)
    if parsed.path == '/anchors':
        name = 'initial' if server.phase == 'initial' else ('revised' if server.phase == 'revised' else 'complete')
        return 200, 'application/json', (server.fixtures / ('anchors-' + name + '.json')).read_bytes()
    if parsed.path != '/samples':
        return None
    if server.phase == 'unavailable':
        return 503, 'application/json', b'{"error":"offline"}'
    query = urllib.parse.parse_qs(parsed.query)
    try:
        boot = query['boot'][0]
        start, stop = int(query['from'][0]), int(query['until'][0])
        if boot not in {'A', 'B'}:
            return 404, 'application/json', b'{"error":"boot"}'
    except (KeyError, ValueError):
        return 400, 'application/json', b'{"error":"interval"}'
    samples = [{'tick': tick, 'value': str(20 + tick // 10)} for tick in range(0, 101 if boot == 'A' else 81, 10 if boot == 'A' else 20) if start <= tick <= stop]
    if server.phase == 'conflict' and boot == 'A' and start <= 20 <= stop:
        samples.append({'tick': 20, 'value': '99'})
        samples.sort(key=lambda row: row['tick'])
    body = {'bootId': boot, 'revision': 'raw1', 'samples': samples[:3], 'truncated': len(samples) > 3}
    return 200, 'application/json', json.dumps(body).encode()
