import type { LiveContext, } from "./live-context.js";

export async function gitPython(
	ctx: LiveContext,
	directory: HostDirectory,
	label: string,
	script: string,
): Promise<string> {
	const file = await ctx.writeFile(`git-${directory.nonce}-${label}.py`, script,);
	const result = await ctx.run<{ success: boolean; output: string; }>([
		"code",
		"run",
		"--file",
		file,
		"--timeout",
		"120000",
	], { projectKey: directory.projectKey, },);
	if (!result.success) throw new Error(`Git fixture operation failed: ${label}`,);
	return result.output.trim();
}
export async function provisionGit(
	ctx: LiveContext,
	directory: HostDirectory,
	branches: Record<string, Record<string, string>> = {},
): Promise<string> {
	const repo = `${directory.path}/fixture.git`;
	const config = JSON.stringify({ repo, work: `${directory.path}/work`, branches, },);
	await gitPython(
		ctx,
		directory,
		"init",
		[
			"import json, pathlib, subprocess",
			`c=json.loads(${JSON.stringify(config,)})`,
			"def git(*args): subprocess.run(['git',*args],check=True,capture_output=True)",
			"git('init','--bare','--initial-branch=main',c['repo'])",
			"if c['branches']:",
			" git('init','--initial-branch=main',c['work'])",
			" git('-C',c['work'],'config','user.name','SDK live')",
			" git('-C',c['work'],'config','user.email','live@example.invalid')",
			" for branch,files in c['branches'].items():",
			"  if branch != 'main': git('-C',c['work'],'checkout','-b',branch)",
			"  for name,value in files.items():",
			"   target=pathlib.Path(c['work'],name)",
			"   assert target.resolve().is_relative_to(pathlib.Path(c['work']).resolve())",
			"   target.parent.mkdir(parents=True,exist_ok=True)",
			"   target.write_text(value)",
			"  git('-C',c['work'],'add','--all')",
			"  git('-C',c['work'],'commit','-m',branch)",
			"  git('-C',c['work'],'push',c['repo'],branch)",
		].join("\n",),
	);
	return repo;
}
export async function readBare(
	ctx: LiveContext,
	directory: HostDirectory,
	repo: string,
	args: string[],
): Promise<string> {
	const command = JSON.stringify(["git", `--git-dir=${repo}`, ...args,],);
	return gitPython(
		ctx,
		directory,
		"read",
		`import json,subprocess\nprint(subprocess.check_output(json.loads(${
			JSON.stringify(command,)
		}),text=True).strip())\n`,
	);
}
export interface HostDirectory {
	path: string;
	nonce: string;
	projectKey: string;
}
export interface HostDaemon {
	pid: number;
	startTime: string;
	port: number;
	repositories: string[];
}
export interface OwnedHostDirectory extends HostDirectory {
	runId: string;
	state: "pending" | "bound" | "deleted";
	daemon?: HostDaemon;
}

export function hostDirectoryScript(
	directory: OwnedHostDirectory,
	operation: "create" | "delete" | "start" | "stop" | "verify",
	repositories: readonly string[] = [],
): string {
	const config = { ...directory, operation, repositories, serverSource: GIT_HTTP_PYTHON, };
	return `import json\nconfig = json.loads(${
		JSON.stringify(JSON.stringify(config,),)
	})\n${HOST_PYTHON}`;
}

const HOST_PYTHON = String
	.raw`import json, os, pathlib, shutil, signal, socket, stat, subprocess, sys, time
p = config['path']
nonce = config['nonce']
assert isinstance(nonce, str) and len(nonce) == 32 and all(c in "0123456789abcdef" for c in nonce)
assert isinstance(config["runId"], str) and len(config["runId"]) == 16 and all(c in "0123456789abcdef" for c in config["runId"])
assert p == '/tmp/sdk_live_' + config['runId'] + '_' + nonce
assert os.path.realpath('/tmp') == '/tmp'
owner = p + '/.owner'
receipt = p + '/.daemon.json'
def regular_json(file):
    fd = os.open(file, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as f:
        assert stat.S_ISREG(os.fstat(f.fileno()).st_mode), "Not a regular ownership file"
        return json.load(f)
def verify():
    assert stat.S_ISDIR(os.lstat(p).st_mode) and os.path.realpath(p) == p, 'Directory identity changed'
    assert regular_json(owner) == {'nonce': nonce, 'runId': config['runId']}, 'Owner marker changed'
def proc(pid):
    try:
        with open('/proc/%s/stat' % pid) as f:
            fields = f.read().rsplit(')', 1)[1].split()
        with open('/proc/%s/cmdline' % pid, 'rb') as f:
            command = f.read().decode().rstrip('\0').split('\0')
        return fields[19], fields[0], int(fields[2]), command
    except (FileNotFoundError, ProcessLookupError):
        return None
def command_for(port, repos):
    assert isinstance(port, int) and 1024 <= port <= 65535
    assert repos and len(set(repos)) == len(repos)
    for repo in repos:
        assert isinstance(repo, str) and pathlib.PurePosixPath(repo).parent == pathlib.PurePosixPath(p)
        assert os.path.realpath(repo) == repo and stat.S_ISDIR(os.lstat(repo).st_mode)
    binary = subprocess.check_output(["git", "--exec-path"], text=True).strip() + "/git-http-backend"
    assert os.access(binary, os.X_OK), "git-http-backend is unavailable"
    return [sys.executable, p + "/git-http.py", str(port), p, binary] + repos
def group_live(pid):
    for name in os.listdir('/proc'):
        if not name.isdigit(): continue
        try:
            with open('/proc/%s/stat' % name) as f: fields = f.read().rsplit(')', 1)[1].split()
            if int(fields[2]) == pid and fields[0] != 'Z': return True
        except (FileNotFoundError, ProcessLookupError): pass
    return False
def stop():
    if not os.path.lexists(receipt):
        assert not config.get('daemon'), 'Daemon receipt missing'
        return
    d = regular_json(receipt)
    assert d['nonce'] == nonce
    pid = d['pid']
    assert isinstance(pid, int) and pid > 1
    expected = command_for(d['port'], d['repositories'])
    if config.get('daemon'):
        assert all(config['daemon'][k] == d[k] for k in ('pid', 'startTime', 'port', 'repositories')), 'Daemon receipt changed'
    info = proc(pid)
    if info:
        assert info[0] == d['startTime'] and info[2] == pid, 'Daemon process identity changed'
        assert info[1] == 'Z' or info[3] == expected, 'Daemon command changed'
        if group_live(pid): os.killpg(pid, signal.SIGTERM)
        until = time.monotonic() + 10
        while group_live(pid):
            assert time.monotonic() < until, 'Daemon process group did not stop'
            time.sleep(0.05)
    else:
        assert not group_live(pid), 'Unverifiable daemon descendants remain'
    os.unlink(receipt)
op = config['operation']
if op == 'create':
    os.mkdir(p, 0o755)
    try:
        fd = os.open(owner, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as f: json.dump({"nonce": nonce, "runId": config["runId"]}, f)
        os.chmod(p, 0o755)
    except BaseException:
        shutil.rmtree(p)
        raise
else:
    if not os.path.lexists(p):
        d = config.get('daemon')
        assert not d or not proc(d['pid']), 'Directory missing while daemon may be running'
        assert op == 'delete', 'Owned directory missing'
    else:
        verify()
        if op == 'start':
            assert not os.path.lexists(receipt), 'Daemon already recorded'
            repos = config['repositories']
            for repo in repos:
                command_for(12345, [repo])
                assert subprocess.check_output(['git', '--git-dir=' + repo, 'rev-parse', '--is-bare-repository'], text=True).strip() == 'true'
                subprocess.run(['git', '--git-dir=' + repo, 'config', 'http.receivepack', 'true'], check=True)
            with open(p + '/git-http.py', 'x') as server: server.write(config['serverSource'])
            with socket.socket() as s:
                s.bind(('127.0.0.1', 0))
                port = s.getsockname()[1]
            command = command_for(port, repos)
            bootstrap = "import json,os,sys; c=json.loads(sys.argv[1]); pid=os.getpid(); start=open('/proc/self/stat').read().rsplit(')',1)[1].split()[19]; d=dict(pid=pid,startTime=start,port=c['port'],repositories=c['repositories'],nonce=c['nonce']); f=os.fdopen(os.open(c['receipt'],os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600),'w'); json.dump(d,f); f.flush(); os.fsync(f.fileno()); f.close(); os.execv(c['command'][0],c['command'])"
            child = subprocess.Popen([sys.executable, '-c', bootstrap, json.dumps(dict(command=command,port=port,repositories=repos,nonce=nonce,receipt=receipt))], start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            until = time.monotonic() + 10
            while True:
                if child.poll() is not None: raise RuntimeError('Git daemon failed to start')
                try:
                    d = regular_json(receipt)
                    info = proc(d['pid'])
                    if info and info[3] == command:
                        with socket.create_connection(('127.0.0.1', port), timeout=0.2): pass
                        break
                except (FileNotFoundError, ConnectionError, TimeoutError, json.JSONDecodeError): pass
                assert time.monotonic() < until, 'Git daemon startup timed out'
                time.sleep(0.05)
            print('HOST_RESULT=' + json.dumps(d))
        elif op == 'verify':
            d = regular_json(receipt)
            assert config.get('daemon') and all(config['daemon'][k] == d[k] for k in ('pid','startTime','port','repositories'))
            info = proc(d['pid'])
            assert info and info[0] == d['startTime'] and info[1] != 'Z' and info[2] == d['pid'] and info[3] == command_for(d['port'], d['repositories']), 'Daemon identity changed'
        elif op in ('stop', 'delete'):
            stop()
            if op == 'delete':
                shutil.rmtree(p)
                assert not os.path.lexists(p)
        else: raise ValueError('Unknown host operation')
print('HOST_OK')
`;

const GIT_HTTP_PYTHON = String.raw`import http.server, json, os, subprocess, sys, urllib.parse
port, root, backend, repos = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4:]
allowed = {'/' + os.path.basename(repo): repo for repo in repos}
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self): self.serve_git()
    def do_POST(self): self.serve_git()
    def serve_git(self):
        self.connection.settimeout(30)
        url = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(url.path)
        prefix, separator, suffix = path[1:].partition('/')
        repo = allowed.get('/' + prefix)
        if not repo or not separator or suffix not in ('info/refs', 'git-upload-pack', 'git-receive-pack') or os.path.realpath(repo) != repo:
            self.send_error(404); return
        body = bytearray()
        limit = 64 * 1024 * 1024
        if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            while True:
                size = int(self.rfile.readline(128).split(b';', 1)[0], 16)
                if size < 0 or len(body) + size > limit: self.send_error(413); return
                if not size:
                    while self.rfile.readline(8192) not in (b'\r\n', b'\n', b''): pass
                    break
                chunk = self.rfile.read(size)
                if len(chunk) != size or self.rfile.read(2) != b'\r\n': self.send_error(400); return
                body.extend(chunk)
        else:
            size = int(self.headers.get('Content-Length', '0'))
            if size < 0 or size > limit: self.send_error(413); return
            body.extend(self.rfile.read(size))
            if len(body) != size: self.send_error(400); return
        env = dict(os.environ, GIT_PROJECT_ROOT=root, GIT_HTTP_EXPORT_ALL='1', PATH_INFO=path,
                   QUERY_STRING=url.query, REQUEST_METHOD=self.command, REMOTE_ADDR='127.0.0.1',
                   CONTENT_TYPE=self.headers.get('Content-Type', ''), CONTENT_LENGTH=str(len(body)),
                   HTTP_GIT_PROTOCOL=self.headers.get('Git-Protocol', ''))
        result = subprocess.run([backend], input=body, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=120)
        headers, split, output = result.stdout.partition(b'\r\n\r\n')
        if result.returncode or not split: self.send_error(502); return
        pairs = [line.decode('latin1').split(':', 1) for line in headers.split(b'\r\n')]
        status = next((int(value.strip().split()[0]) for key, value in pairs if key.lower() == 'status'), 200)
        self.send_response(status)
        for key, value in pairs:
            if key.lower() not in ('status', 'content-length', 'connection'): self.send_header(key, value.strip())
        self.send_header('Content-Length', str(len(output)))
        self.end_headers()
        self.wfile.write(output)
http.server.ThreadingHTTPServer(('127.0.0.1', port), Handler).serve_forever()
`;
