'use strict';
var log = require('./../logger')().log;
var utils = require('./../utils');
var storj = require('storj-lib');
var async = require('async');
var Whitelist = require('storj-lib/lib/bridge-client/whitelist');
var os = require('os');
var fs = require('graceful-fs');
var path = require('path');
var platform = os.platform();
var HOME = platform !== 'win32' ? process.env.HOME : process.env.USERPROFILE;

module.exports.list = function(bucketid) {
  var filelist = JSON.parse(fs.readFileSync(path.join(HOME, '.storjcli/.files')));
  var client = this._storj.PrivateClient();
  bucketid = this._storj.getRealBucketId(bucketid);

  client.listFilesInBucket(bucketid, function(err, files) {
    if (err) {
      return log('error', err.message);
    }

    if (!files.length) {
      return log('warn', 'There are no files in this bucket.');
    }

    files.forEach(function(file) {
      log(
        'info',
        'Name: %s, Type: %s, Size: %s bytes, ID: %s',
        [file.filename, file.mimetype, file.size, file.id]
      );
      filelist[file.id] = {};
      filelist[file.id]['id'] = file.id;
      filelist[file.id]['bucket'] = bucketid;
      filelist[file.id]['download'] = 0;
      filelist[file.id]['error'] = 0;
    });
    
    fs.writeFileSync(path.join(HOME, '.storjcli/.files'), JSON.stringify(filelist, null, "\t"));
  });
};

module.exports.getInfo = function(bucketid, fileid) {
   var client = this._storj.PrivateClient();
   bucketid = this._storj.getRealBucketId(bucketid);
   fileid = this._storj.getRealFileId(bucketid, fileid);

  client.getFileInfo(bucketid, fileid, function(err, file) {
    if (err) {
      return log('error', err.message);
    }

    log(
      'info',
      'Name: %s, Type: %s, Size: %s bytes, ID: %s',
      [file.filename, file.mimetype, file.size, file.id]
    );
  });
};

module.exports.listMirrors = function(bucketid, fileid) {
   var client = this._storj.PrivateClient();
   bucketid = this._storj.getRealBucketId(bucketid);
   fileid = this._storj.getRealFileId(bucketid, fileid);

  client.listMirrorsForFile(bucketid, fileid, function(err, mirrors) {
    if (err) {
      return log('error', err.message);
    }

    mirrors.forEach((s, i) => {
      log('info', '');
      log('info', 'Established');
      log('info', '-----------');
      log('info', 'Shard: %s', [i]);
      s.established.forEach((s, i) => {
        if (i === 0) {
          log('info', 'Hash: %s', [s.shardHash]);
        }
        log('info', '    %s', [storj.utils.getContactURL(s.contact)]);
      });
      log('info', '');
      log('info', 'Available');
      log('info', '---------');
      log('info', 'Shard: %s', [i]);
      s.available.forEach((s, i) => {
        if (i === 0) {
          log('info', 'Hash: %s', [s.shardHash]);
        }
        log('info', '    %s', [storj.utils.getContactURL(s.contact)]);
      });
    });
  });
};

module.exports.remove = function(id, fileId, env) {
  var client = this._storj.PrivateClient();
  var keypass = this._storj.getKeyPass();
  id = this._storj.getRealBucketId(id);
  fileId = this._storj.getRealFileId(id, fileId);

  function destroyFile() {
    utils.getKeyRing(keypass, function(keyring) {
      client.removeFileFromBucket(id, fileId, function(err) {
        if (err) {
          return log('error', err.message);
        }

        log('info', 'File was successfully removed from bucket.');
        keyring.del(fileId);
      });
    });
  }

  if (!env.force) {
    return utils.getConfirmation(
      'Are you sure you want to destroy the file?',
      destroyFile
    );
  }

  destroyFile();
};

module.exports.stream = function(bucket, id, env) {
  var self = this;
  var client = this._storj.PrivateClient({
    logger: storj.deps.kad.Logger(0)
  });
  var keypass = this._storj.getKeyPass();
  bucket = this._storj.getRealBucketId(bucket);
  id = this._storj.getRealFileId(bucket, id);

  utils.getKeyRing(keypass, function(keyring) {
    var secret = keyring.get(id);

    if (!secret) {
      return log('error', 'No decryption key found in key ring!');
    }

    var decrypter = new storj.DecryptStream(secret);
    var exclude = env.exclude.split(',');

    client.createFileStream(bucket, id, function(err, stream) {
      if (err) {
        return process.stderr.write(err.message);
      }

      stream.on('error', function(err) {
        log('warn', 'Failed to download shard, reason: %s', [err.message]);

        if (!err.pointer) {
          return;
        }

        log('info', 'Retrying download from other mirrors...');
        exclude.push(err.pointer.farmer.nodeID);
        module.exports.stream.call(
          self,
          bucket,
          id,
          { exclude: env.exclude.join(',') }
        );
      }).pipe(decrypter).pipe(process.stdout);
    });
  });
};

module.exports.getpointers = function(bucket, id, env) {
  var client = this._storj.PrivateClient();
  bucket = this._storj.getRealBucketId(bucket);
  id = this._storj.getRealFileId(bucket, id);

  client.createToken(bucket, 'PULL', function(err, token) {
    if (err) {
      return log('error', err.message);
    }

    var skip = Number(env.skip);
    var limit = Number(env.limit);

    client.getFilePointers({
      bucket: bucket,
      file: id,
      token: token.token,
      skip: skip,
      limit: limit
    }, function(err, pointers) {
      if (err) {
        return log('error', err.message);
      }

      if (!pointers.length) {
        return log('warn', 'There are no pointers to return for that range');
      }

      log('info', 'Listing pointers for shards %s - %s', [
        skip, skip + pointers.length - 1
      ]);
      log('info', '-----------------------------------------');
      log('info', '');
      pointers.forEach(function(location, i) {
        log('info', 'Index:  %s', [skip + i]);
        log('info', 'Hash:   %s', [location.hash]);
        log('info', 'Token:  %s', [location.token]);
        log('info', 'Farmer: %s', [
          storj.utils.getContactURL(location.farmer)
        ]);
        log('info', '');
      });
    });
  });
};

module.exports.getallpointers = function(bucket, env) {
  var start = Date.now();
  var client = this._storj.PrivateClient();
  
  var filelist = JSON.parse(fs.readFileSync(path.join(HOME, '.storjcli/.files')));
  var whitelist = new Whitelist(path.join(HOME, '.storjcli'));
  
  var error = 0;
  var download = 0;
    
  async.forEachLimit(filelist, 300, function(file, callback) {
    
    if ( file.download >= 50000 || file.error >= 50 || (file.download === 0 && file.error >= 10) ) {
      if ( file.download === 0 || file.download >= 50000 ) {
        delete filelist[file.id];
        fs.writeFileSync(path.join(HOME, '.storjcli/.files'), JSON.stringify(filelist, null, "\t"));
      }
      return callback(null);
    }
    
    client.createToken(file.bucket, 'PULL', function(err, token) {
      
      if (err) {
        log('warn', 'Create Token: %s', err.message);
        error++;
        filelist[file.id]['error']++;
        fs.writeFileSync(path.join(HOME, '.storjcli/.files'), JSON.stringify(filelist, null, "\t"));
        return callback(null);
      }
            
      var skip = Number(env.skip);
      var limit = Number(env.limit);

      client.getFilePointers({
        bucket: file.bucket,
        file: file.id,
        token: token.token,
        skip: skip,
        limit: limit
      }, function(err, pointers) {
        if (err) {
          log('warn', 'Get Pointer: %s', err.message);
          error++;
          filelist[file.id]['error']++;
          fs.writeFileSync(path.join(HOME, '.storjcli/.files'), JSON.stringify(filelist, null, "\t"));
          return callback(null);
        }
        
        if (!pointers.length) {
          log('warn', 'There are no pointers to return for that range');
          error++;
          return callback(null);
        }
        
        pointers.forEach(function(location, i) {
          if ( whitelist.toObject().indexOf(location.farmer.nodeID) === -1 ) {
            filelist[file.id]['error'] = 999;
          } else {
            whitelist.push(location.farmer.nodeID);
            var counter = whitelist.getValue(location.farmer.nodeID)
            download++;
            filelist[file.id]['error'] = 0;
            filelist[file.id]['download'] = counter;
            filelist[file.id]['farmer'] = location.farmer.nodeID;
            log('info', 'Farmer: %s Count: %s', [location.farmer.nodeID, counter]);
          }
        });
        
        return callback(null);
      });
    });
  }, function(err) {
    fs.writeFileSync(path.join(HOME, '.storjcli/.files'), JSON.stringify(filelist, null, "\t"));
    log('info', 'Downloads: %s Errors: %s Zeit: %s', [download, error, Date.now() - start]);
  });
};
