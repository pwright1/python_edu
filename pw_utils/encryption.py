
# Copyright Aug 2025, Philip Wright. All rights reserved. 

import subprocess
import os
from filelock import FileLock, Timeout
from pw_utils import mydb_utils


def gpg_encrypt(filename, touser="dukeuga@serotinalsw.com"):
    
    ret = 0
    if not os.path.isfile(filename):
        raise Exception(f"Can't find file {filename} to encrypt")
    if len(filename) >= 5:
        ext = filename[-4:]
        if ext.lower() in [".asc",".gpg",".pgp"]:
            raise Exception(f"Can't encrypt file {filename} with extension {ext}")
    efilename = f"{filename}.gpg"
    
    cmd = f"gpg -q --batch -e -r {touser} -o \"{efilename}\" \"{filename}\""
    ret =  os.system(cmd)
    if ret == 0 and os.path.isfile(efilename):
        try:
            os.remove(filename)
        except Exception as e:
            print(f"exception on file delete {e}")
    print(ret)


def gpg_decrypt(efilename, the_pass):
    #if not os.path.isfile(efilename):
    #    raise Exception(f"Can't find file {efilename} to decrypt")
    if len(efilename) >= 5:
        ext = efilename[-4:]
        print("{} {}".format(efilename,ext))
        if ext.lower() not in [".asc",".gpg",".pgp"]:
            raise Exception(f"Can't decrypt file {efilename} without extension .asc | .gpg | .pgp")
    args = []
    args.append("gpg")
    args.append("--no-mdc-warning")
    args.append("-q")
    args.append("--batch")
    args.append("--passphrase-fd")
    args.append("0")
    args.append("--pinentry-mode")
    args.append("loopback")
    args.append("--decrypt")
    args.append(efilename)
    
    result = subprocess.run(args,check=False, timeout=30,input=the_pass,text=True,encoding='utf-8',capture_output=True)
    
    if result.returncode != 0:
        print(f"{result.stderr.strip()}")
        #print(result.stdout)
    
    if result.returncode == 0:
        try:
            os.remove(efilename)
        except Exception as e:
            print(f"exception on file delete {e}")
        
    os.system("gpgconf --kill gpg-agent")
    print(result.returncode)
    return result.returncode

