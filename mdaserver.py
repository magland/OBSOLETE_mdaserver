#!/usr/bin/env python
import SimpleHTTPServer
import SocketServer
import urlparse
import subprocess
import os
import sys
import ConfigParser
import shutil
from json import JSONDecoder, JSONEncoder
from exceptions import ValueError

class ConfigReader(object):
    def __init__(self, **kwargs):
        self.defaults = kwargs

    def read(self, path):
        reader = ConfigParser.ConfigParser(allow_no_value=True, defaults=self.defaults)
        reader.read(path)
        return reader

#example call:
#http://localhost:8000/firings.mda?a=readChunk&size=5,100

class MyRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def translate_path(self, path):
        if ".." in path: #to be safe?
            return ""
        path=self.cfg("mdachunk_data_path")+path
	return path

    def do_GET(self): #handle a GET request
        request_path = urlparse.urlparse(self.path).path  # sanity check needed

        methods = {
            'size': self.handle_SIZE,
            'info': self.handle_INFO,
            'readChunk': self.handle_READCHUNK
        }
        method = self.query('a')
        if method and methods.has_key(method):
            response = {"request": {"method": method, "mda": request_path } }
            try:
                result = methods.get(method)(request_path)
                response["result"] = result
            except ValueError as e:
                response["error"] = str(e)
            if self.query('output') == "text":
                if response.has_key("result"):
                    self.send_plain_text(response["result"])
                elif response.has_key("error"):
                    self.send_plain_text("ERROR: "+response["error"])
            else:
                encoded = JSONEncoder(indent=4).encode(response)
                self.send_plain_text(encoded)
        else:
            SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def handle_SIZE(self, path):
        # let's cheat :)
        response = self.handle_INFO(path)
        if self.query("output") == "text": return response.split('\n')[0]
        return { 'dimensions': response["dimensions"] }

    def handle_INFO_TEXT(self, path):
        mdachunk_exe        = self.cfg("mdachunk_exe")
        mda_path            = self.cfg("mda_path")
        mda_fname=mda_path+"/"+path
        (str,exit_code)=self.call_and_read_output(" ".join([mdachunk_exe, "info", mda_fname]))
        return str

    def handle_INFO(self, path):
        #str = self.handle_INFO_TEXT(path)
        str = "5,52355,1,1,1,1\n8dc26fa7b25afaf114e2188a8f62db1a99e2c35b\n1459597809000"
        if self.query("output") == "text": return str
        # it would be easier if mdachunk spitted out JSON
        # but hey, let's parse the response ourselves
        # three lines:
        lines = str.split('\n')
        if len(lines) < 3: raise ValueError("Incorrect output from mdachunk")
        # 6 integers separated by commas
        dimensions = lines[0].split(',')
        if len(dimensions) != 6: raise ValueError("Incorrect output from mdachunk")
        # sha1
        sha1 = lines[1]
        # path
        filepath = lines[2]
        return {
            'dimensions': dimensions,
            'sha1': sha1,
            'path': filepath
        }

    def handle_READCHUNK(self, path):
        mdachunk_exe        = self.cfg("mdachunk_exe")
        mdachunk_data_path  = self.cfg("mdachunk_data_path")
        mdachunk_data_url   = self.cfg("mdachunk_data_url")
        mda_path            = self.cfg("mda_path")
        mda_fname=mda_path+"/"+path

        datatype=self.query("datatype","float32")
        index=self.query("index","0")
        size=self.query("size","")
        self.mkdir_if_needed(mdachunk_data_path)
        outpath=self.query("outpath",mdachunk_data_path)
        cmd = " ".join([
            mdachunk_exe,
            "readChunk",
            mda_fname,
            "--index="+index,
            "--size="+size,
            "--datatype="+datatype,
            "--outpath="+outpath
        ])
        (str,exit_code)=self.call_and_read_output(cmd)
        if not exit_code:
            url0=mdachunk_data_url+"/"+str
            if self.query("output") == "text": return url0
            return { 'path': url0 }
        else:
            raise ValueError(str)

    def cfg(self, key, section = 'General'):
        return self.config.get(section, key)

    def query(self,field,defaultval=""): #for convenience
    	parts=urlparse.urlparse(self.path)
    	query=urlparse.parse_qs(parts.query)
    	tmp=query.get(field,[defaultval])
        return tmp[0] if tmp else ""

    def call_and_read_output(self,cmd): #make a system call and return the output string and exit code
    	print(cmd)
    	process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    	exit_code = process.wait()
    	(out,err)=process.communicate()
    	process.stderr.close()
    	process.stdout.close()
        ret = "ERROR: {}".format(out) if exit_code else out

    	print(out)
    	return (out,exit_code)

    def mkdir_if_needed(self,path): #for convenience
		if not os.path.exists(path):
			os.makedirs(path)

    def send_plain_text(self,txt): #send a plain text response
		self.send_response(200)
		self.send_header("Content-type", "text/html")
		self.send_header("Content-length", len(txt))
		self.end_headers()
		self.wfile.write(txt)
	

class MyTCPServer(SocketServer.TCPServer):
    allow_reuse_address = True

def main():
    config_fname='mdaserver.cfg'
    example_config_fname='mdaserver.example.cfg'
    if not os.path.isfile(config_fname):
        shutil.copyfile(example_config_fname,config_fname)
        print("Please edit the configuration file "+config_fname+" and then re-run this program")
        return
    Handler = MyRequestHandler
    port = sys.argv[1] if sys.argv[1:] else "8000"
    config = ConfigReader(port=port).read(config_fname)
    Handler.config = config
    server = MyTCPServer(('0.0.0.0', int(config.get('General', 'port'))), Handler)
    server.serve_forever()

if __name__ == "__main__":
    main()
