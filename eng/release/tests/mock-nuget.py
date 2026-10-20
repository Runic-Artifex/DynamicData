#!/usr/bin/env python3
"""Offline stand-in for the public nuget.org endpoints that eng/release/nuget.py reads.

Usage: mock-nuget.py <root> <port-file>

Each test case is a directory under <root>; its service index is
http://127.0.0.1:<port>/<case>/v3/index.json. A case directory holds
  packages/<lower id>/<lower version>.nupkg   published packages (flat container)
  owners/<lower id>                           one owner per line; makes search list the ID
Every request is appended to <root>/requests.log with any credential header it carried.
"""

import http.server
import json
from pathlib import Path
import sys
import urllib.parse

root = Path(sys.argv[1])
CREDENTIAL_HEADERS = ('authorization', 'x-nuget-apikey')


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def send(self, status, body=b'', content_type='application/json'):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, value):
        self.send(200, json.dumps(value).encode())

    def do_GET(self):
        url = urllib.parse.urlsplit(self.path)
        credentials = [name for name in CREDENTIAL_HEADERS if self.headers.get(name)]
        with open(root / 'requests.log', 'a', encoding='utf-8') as log:
            log.write(f'{self.path} credentials={",".join(credentials) or "none"}\n')
        parts = url.path.strip('/').split('/')
        if len(parts) < 2 or not (root / parts[0]).is_dir():
            return self.send(404)
        case, rest = root / parts[0], parts[1:]
        base = f'http://127.0.0.1:{self.server.server_port}/{parts[0]}'
        if rest == ['v3', 'index.json']:
            return self.send_json({'version': '3.0.0', 'resources': [
                {'@id': f'{base}/query', '@type': 'SearchQueryService/3.5.0'},
                {'@id': f'{base}/flat/', '@type': 'PackageBaseAddress/3.0.0'},
            ]})
        if rest == ['query']:
            query = urllib.parse.parse_qs(url.query).get('q', [''])[0]
            package_id = query.removeprefix('packageid:').lower()
            owners = case / 'owners' / package_id
            data = []
            if package_id and owners.is_file():
                data.append({'id': query.removeprefix('packageid:'),
                             'owners': [line for line in owners.read_text().splitlines() if line]})
            return self.send_json({'totalHits': len(data), 'data': data})
        if rest[0] == 'flat' and len(rest) == 3 and rest[2] == 'index.json':
            directory = case / 'packages' / rest[1]
            if not directory.is_dir():
                return self.send(404)
            return self.send_json({'versions': sorted(p.stem for p in directory.glob('*.nupkg'))})
        if rest[0] == 'flat' and len(rest) == 4:
            package = case / 'packages' / rest[1] / f'{rest[2]}.nupkg'
            if rest[3] != f'{rest[1]}.{rest[2]}.nupkg' or not package.is_file():
                return self.send(404)
            return self.send(200, package.read_bytes(), 'application/octet-stream')
        return self.send(404)


server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
port_file = Path(sys.argv[2])
port_file.with_suffix('.tmp').write_text(str(server.server_port))
port_file.with_suffix('.tmp').replace(port_file)
server.serve_forever()
