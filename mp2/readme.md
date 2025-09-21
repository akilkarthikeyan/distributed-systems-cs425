# File Locations
The location of some files are hardcoded.

The server will look for log files from /home/shared/logs/vm#.log

The server will resolve the file name of log files based on the hostname of itself. for example fa25-cs425-6410.cs.illinois.edu will only look for vm10.log

The client will look for a list of server addresses from /home/shared/sources.json

# Run the server/client program
either compile /server/server.go and /client/client.go or use "go run" to run the programs

the server program accepts one flag: port number (default to port 1234)
for example server -port 5000 will start a server at port 5000

the client program takes the same arguments as the grep command, execpt the file path will be appended by the server
for example (if the executable is named rgrep): rgrep pattern, or with given flags: rgrep -i pattern
the client will query all machines whose addresses are specified in /home/shared/sources.json