'use strict';
var log = require('./../logger')().log;
var utils = require('./../utils');
var fs = require('fs');
var path = require('path');
var globule = require('globule');
var async = require('async');
var storj = require('storj-lib');
var assert = require('assert');
var monitor = require('os-monitor');

/**
 * Interface for uploading files to Storj Network
 * @constructor
 * @license AGPL-3.0
 * @param {BridgeClient} client - Authenticated Bridge Client with Storj API.
 * @param {String} keypass - Password for unlocking keyring.
 * @param {Number} options.env.concurrency - shard upload concurrency.
 * @param {Number} options.env.fileconcurrency - File upload concurrency.
 * @param {Number} options.env.redundancy - Number of mirrors per shard.
 * @param {String} options.bucket - Bucket files are uploaded to.
 * @param {String} options.filepath - Path of files being uploaded.
 */
  /* jshint maxstatements: 20 */
function Uploader(client, keypass, options) {
  if (!(this instanceof Uploader)) {
    return new Uploader(client, keypass, options);
  }

  this.shardConcurrency = options.env.concurrency ?
                    parseInt(options.env.concurrency) :
                    6;
  this.fileConcurrency = options.env.fileconcurrency || 1;
  this.bucket = options.bucket;
  this.redundancy = options.env.redundancy || 0;
  this.client = client({ transferConcurrency: this.shardConcurrency});
  this.keypass = keypass();
  this.filepaths = this._getAllFiles(options.filepath);
  this.fileCount = this.filepaths.length;
  this.uploadedCount = 0;
  this.fileMeta = [];
  this.nextFileCallback = {};

  this._validate();

  log('info', '%s file(s) to upload.', [ this.fileCount ]);
}

/**
 * Validate all required parameters were passed to constructor.
 * @private
 */
Uploader.prototype._validate = function() {
  if (this.fileConcurrency > 6) {
    log(
      'warn',
      'A file concurrency of %s may result in issues!',
      [ this.fileConcurrency ]
    );
  }

  if (this.redundancy === 0) {
    log(
      'warn',
      'A redundancy of %s means files will not be mirrored!',
      [ this.redundancy ]
    );
  }

  assert(this.fileConcurrency >= 1, 'File Concurrency cannot be less than 1');
  assert(
    ((parseInt(this.redundancy) <= 12) || (parseInt(this.redundancy) >= 0)),
    this.redundancy + ' is an invalid Redundancy value.'
  );
  assert(this.fileCount >= 1, '0 files specified to be uploaded.');
};

/**
 * Parse options.filepath to determine if multiple files are being uploaded
 * and check if they exist.
 * @param {String} filepath - File Glob to be uploaded
 * @private
 */
Uploader.prototype._getAllFiles = function(filepath) {
  var filepaths = process.argv.slice();
  var firstFileIndex = filepaths.indexOf(filepath);
  filepaths.splice(0,firstFileIndex);
  var expandedFilepaths = [];

  filepaths.forEach(function(file) {
    // In *NIX the wildcard is already parsed so this will cover other OS's
    var parsedFileArray = globule.find(file);

    if (!storj.utils.existsSync(parsedFileArray[0])) {
      throw new Error(file + ' could not be found');
    }

    if (fs.statSync(parsedFileArray[0]).isFile() === true) {
      try {
        fs.accessSync(parsedFileArray[0], fs.R_OK);
      } catch (err) {
        throw err;
      }

      expandedFilepaths = expandedFilepaths.concat(parsedFileArray);
    }
  });

  return expandedFilepaths;
};

/**
 * Clean up tmp files created by Uploader.prototype._makeTempDir
 * @param {String} filename - file name with tmp files that need to be cleaned.
 * @param {Function} tmpCleanup - function created by _makeTempDir for cleaning.
 * @private
 */
Uploader.prototype._cleanup = function(filename, tmpCleanup) {
  log('info', '[ %s ] Cleaning up...', filename);
  tmpCleanup();
  log('info', '[ %s ] Finished cleaning!', filename);
};

/**
 * set this.keyring using this.keypass
 * @private
 */
Uploader.prototype._getKeyRing = function(callback) {
  var self = this;

  utils.getKeyRing(this.keypass, function(keyring) {
    self.keyring = keyring;
    callback(null);
    return;
  });
};

/**
 * Begin looping through files to be uploaded
 * @private
 */
Uploader.prototype._loopThroughFiles = function(callback) {
  var self = this;

  log('info', 'Generating encryption key...');

  async.eachLimit(
    self.filepaths,
    self.fileConcurrency,
    function(filepath, nextFileCallback) {
      if (!storj.utils.existsSync(filepath)) {
        callback('No file found.', filepath);
        return;
      }

      self.nextFileCallback[filepath] = nextFileCallback;

      callback(null, filepath);
    }
  );
};

/**
 * Create temp dir for storing encrypted versions of files to be uploaded.
 * @param {String} filepath - file to be uploaded
 * @private
 */
Uploader.prototype._makeTempDir = function(filepath, callback) {
  var self = this;

  utils.makeTempDir(function(err, tmpDir, tmpCleanup) {
    if (err) {
      callback(err, filepath);
      log('error', 'Unable to create temp directory for file %s', filepath);
      return;
    }

    log('info', 'Encrypting file "%s"', [filepath]);

    var secret = new storj.DataCipherKeyIv();
    var filename = path.basename(filepath);

    self.fileMeta[filepath] = {
      filename: filename,
      tmpDir: tmpDir,
      tmppath: path.join(tmpDir, filename + '.crypt'),
      tmpCleanup: tmpCleanup,
      secret: secret,
      encrypter: new storj.EncryptStream(secret)
    };

    callback(null, filepath);
  });
};

/**
 * encrypt the filepath
  * @param {String} filepath - file to be uploaded
 * @private
 */
Uploader.prototype._createReadStream = function(filepath, callback) {
  var self = this;

  fs.createReadStream(filepath)
    .pipe(self.fileMeta[filepath].encrypter)
    .pipe(fs.createWriteStream(self.fileMeta[filepath].tmppath))
    .on('finish', function() {
      log(
        'info',
        '[ %s ] Encryption complete',
        self.fileMeta[filepath].filename
      );
      callback(null, filepath);
  });
};

/**
 *  Create token for storing file
 * @param {String} filepath - file to be uploaded
 * @private
 */
Uploader.prototype._createToken = function(filepath, callback) {
  var self = this;
  var filename = self.fileMeta[filepath].filename;
  var retry = 0;

  function _createToken() {
    log(
      'info',
      '[ %s ] Creating storage token... (retry: %s)',
      [ filename, retry ]
    );

    self.client.createToken(self.bucket, 'PUSH', function(err, token) {
      if (err) {

        if (retry < 999) {
          retry++;
          return _createToken();
        }

        callback(err, filepath);
        self._cleanup(filename, self.fileMeta[filepath].tmpCleanup);
        return;
      }

      callback(null, filepath, token);
    });
  }

  _createToken();
};

/**
 *  Store encrypted file in bucket
 * @param {String} filepath - file to be uploaded
 * @private
 */
 /* jshint maxstatements: 20 */
Uploader.prototype._storeFileInBucket = function(filepath, token, callback) {
  var self = this;
  var filename = self.fileMeta[filepath].filename;

  log('info', '[ %s ] Storing file, hang tight!', filename);

  self.client.storeFileInBucket(
    self.bucket,
    token.token,
    self.fileMeta[filepath].tmppath,
    function(err, file) {
      if (err) {
        log(
          'warn',
          '[ %s ] Error occurred. Triggering cleanup...',
          filename
         );
        callback(err, filepath);
        return;
      }

      self.keyring.set(file.id, self.fileMeta[filepath].secret);
      self._cleanup(filename, self.fileMeta[filepath].tmpCleanup);
      delete self.fileMeta[filepath];

      log('info', '[ %s ] Encryption key saved to keyring.', filename);
      log('info', '[ %s ] File successfully stored in bucket.', filename);
      log(
        'info',
        'Name: %s, Type: %s, Size: %s bytes, ID: %s',
        [file.filename, file.mimetype, file.size, file.id]
      );

      if (self.redundancy && self.redundancy > 0) {
        return self._mirror(file.id);
      }

      self.uploadedCount++;

      log(
        'info',
        '%s of %s files uploaded',
        [ self.uploadedCount, self.fileCount ]
      );

      if (self.uploadedCount === self.fileCount) {
        log( 'info', 'Done.');
        callback(null, filepath);
      }

      self.nextFileCallback[filepath]();

    }
  );
};

/**
 * Mirror files
 * @param {String} fileid - id of file to be mirrored
 * @private
 */
Uploader.prototype._mirror = function(fileid) {
  log(
    'info',
    'Establishing %s mirrors per shard for redundancy',
    [this.redundancy]
  );

  this.client.replicateFileFromBucket(
    this.bucket,
    fileid,
    parseInt(this.redundancy),
    function(err, replicas) {
      if (err) {
        return log('error', err.message);
      }

      replicas.forEach(function(shard) {
        log('info', 'Shard %s %s mirroring by %s nodes', [
          shard.hash,
          shard.status,
          shard.mirrors
        ]);
      });

    }
  );
};

/**
 *
 *
 * @private
 */
Uploader.prototype._handleFailure = function() {
  var self = this;
  monitor.stop();
  for (var file in self.fileMeta) {
    self._cleanup(
      self.fileMeta[file].filename,
      self.fileMeta[file].tmpCleanup
    );
  }
};

/**
 * Aggregator function for complete upload process.
 * @param {Function} finalCallback - function for handling errors and when done.
 */
Uploader.prototype.start = function(finalCallback) {
  var self = this;

  monitor.start(
    {
      delay: 3000, // interval in ms between monitor cycles
      freemem: 8000000 // freemem under which event 'freemem' is triggered
    }
  );

  // define handler for a too low free memory
  monitor.on('freemem', function() {
    self._handleFailure();
    return finalCallback(new Error('Not enough free memory to continue!'));
  });

  async.waterfall([
    function _getKeyRing(callback) {
      self._getKeyRing(callback);
    },
    function _beginLoop(callback) {
      self._loopThroughFiles(callback);
    },
    function _makeTempDir(filepath, callback) {
      self._makeTempDir(filepath, callback);
    },
    function _createReadStream(filepath, callback) {
      self._createReadStream(filepath, callback);
    },
    function _createToken(filepath, callback) {
      self._createToken(filepath, callback);
    },
    function _storeFileInBucket(filepath, token, callback) {
      self._storeFileInBucket(filepath, token, callback);
    }
  ], function (err, filepath) {
    self._handleFailure();
    finalCallback(err, filepath);
  });

};


module.exports = Uploader;
