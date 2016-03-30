#!/usr/bin/env python
import SimpleHTTPServer
import SocketServer
import urlparse
import urlparse
import subprocess
import os
import sys

#example call:
#http://localhost:8000/firings.mda?a=readChunk&size=5,100

if sys.argv[1:]:
    port = int(sys.argv[1])
else:
    port = 8000

# This needs to go in a separate config file. Witold, what's a good way?
config={}
#path to the mdachunk executable
config["mdachunk_exe"]="/mnt/xfs1/home/magland/dev/mdachunk/bin/mdachunk"
#root directory where we are serving the .mda files
config["mda_path"]="/mnt/xfs1/home/magland/dev/mdaserver/testdata"
#the place where mdachunk will store the chunks of data
config["mdachunk_data_path"]="/mnt/xfs1/home/magland/dev/mdaserver/mdachunk_data"
#the url to the mdachunk_data_path
config["mdachunk_data_url"]="http://localhost:"+str(port)+"/mdachunk_data"

class MyRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self): #handle a GET request
    	mdachunk_exe=config["mdachunk_exe"]
    	mda_path=config["mda_path"]
    	mdachunk_data_path=config["mdachunk_data_path"]
    	mdachunk_data_url=config["mdachunk_data_url"]
    	mda_fname=mda_path+"/"+urlparse.urlparse(self.path).path
    	if (self.query("a")=="size"): #need to return the dimensions of the .mda
    		(str,exit_code)=self.call_and_read_output(mdachunk_exe+" size "+mda_fname)
    		self.send_plain_text(str)
    		return
    	elif (self.query("a")=="readChunk"): #read a chunk and return url to retrieve the .mda binary data
    		print(mda_fname)
    		datatype=self.query("datatype","float32")
    		index=self.query("index","0,0,0")
    		size=self.query("size","")
    		self.mkdir_if_needed(mdachunk_data_path)
    		outpath=self.query("outpath",mdachunk_data_path)
    		cmd=mdachunk_exe+" readChunk "+mda_fname
    		cmd+=" --index="+index
    		cmd+=" --size="+size
    		cmd+=" --datatype="+datatype
    		cmd+=" --outpath="+outpath
    		(str,exit_code)=self.call_and_read_output(cmd)
    		if exit_code==0:
    			url0=mdachunk_data_url+"/"+str
    			self.send_plain_text(url0)
    		else:
    			self.send_plain_text("ERROR: "+str)
    		return
        return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)
    def query(self,field,defaultval=""): #for convenience
    	parts=urlparse.urlparse(self.path)
    	query=urlparse.parse_qs(parts.query)
    	tmp=query.get(field,[defaultval])
    	if (len(tmp)>0):
    		return tmp[0];
    	else:
    		return ""
    def call_and_read_output(self,cmd): #make a system call and return the output string and exit code
    	print(cmd)
    	process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    	exit_code = process.wait()
    	(out,err)=process.communicate()
    	process.stderr.close()
    	process.stdout.close()
    	if exit_code<>0:
    		ret="ERROR: "+out
    	else:
    		ret=out
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
	
Handler = MyRequestHandler
class MyTCPServer(SocketServer.TCPServer):
    allow_reuse_address = True
server = MyTCPServer(('0.0.0.0', port), Handler)

server.serve_forever()