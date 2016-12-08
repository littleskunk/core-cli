'use strict';
/* jshint maxstatements: 30 */
var expect = require('chai').expect;
var proxyquire = require('proxyquire');
var sinon = require('sinon');
var stream = require('stream');

var LoggerStub = {
  log: sinon.stub()
};
var storjStub = {};
var fsStub = {};
var utilsStub = {};

var Downloader = proxyquire('../../bin/actions/downloader.js', {
  './../logger': function() {
    return LoggerStub;
  },
  'storj-lib': storjStub,
  'fs': fsStub,
  './../utils': utilsStub
});

var downloader;
var sandbox;
var clientStub = {};
var testClient = function() {
  return clientStub;
};
var testFileId = 'test file id';
var testBucketId = 'test bucket id';
var testOptions = {
  filepath: '/test/filepath/testfile.extension',
  keypass: 'test keypass',
  env: {
    exclude: 'a,b,c,d'
  }
};

describe('downloader', function() {
  beforeEach(function() {
    LoggerStub.log.reset();
    for (var k in storjStub) {
      try {
        delete storjStub[k];
      } catch(e) {
        // occurs when a key in the object is not writable
      }
    }
  });

  describe('@constructor', function() {
    before(function() {
      sandbox = sinon.sandbox.create();
      Downloader.oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
    });
    beforeEach(function() {
      Downloader.prototype._validate.reset();
    });
    after(function() {
      Downloader.prototype._validate = Downloader.oldValidate;
      delete Downloader.oldValidate;
      sandbox.restore();
    });

    it('should create an instance without the new keyword', function() {
      expect(Downloader(testClient, testFileId, testBucketId, testOptions))
        .to.be.instanceOf(Downloader);
    });

    it('should properly set all instance variables and validate', function() {
      var newDownloader = new Downloader(testClient, testFileId,
        testBucketId, testOptions);
      expect(newDownloader.bucket).to.equal(testBucketId);
      expect(newDownloader.fileid).to.equal(testFileId);
      expect(newDownloader.filepath).to.equal(testOptions.filepath);
      expect(newDownloader.client).to.equal(testClient());
      expect(newDownloader.keypass).to.equal(testOptions.keypass);
      expect(newDownloader.exclude).to.equal(testOptions.env.exclude);
      expect(newDownloader._validate.callCount).to.equal(1);
    });
  });

  describe('#_validate', function() {
    before(function() {
      sandbox = sinon.sandbox.create();
      var oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
      downloader = new Downloader(testClient, testFileId, testBucketId,
        testOptions);
      Downloader.prototype._validate = oldValidate;
      sandbox.spy(Downloader.prototype, '_validate');
    });
    beforeEach(function() {
      Downloader.prototype._validate.reset();
    });
    after(function() {
      sandbox.restore();
    });

    it('should throw an error if the filepath already exists', function() {
      storjStub.utils = {
        existsSync: sinon.stub().returns(true)
      };
      var isFileStub = {
        isFile: sinon.stub().returns(true)
      };
      fsStub.statSync = sinon.stub().returns(isFileStub);

      expect(function() {
        downloader._validate();
      }).to.throw('Refusing to overwrite file at ' + testOptions.filepath);
    });

    it('should throw an error if file directory does not exist', function() {
      storjStub.utils = {
        existsSync: sinon.stub().returns(false)
      };
      var filepath = testOptions.filepath;
      var dirname = filepath.slice(0, filepath.lastIndexOf('/'));

      expect(function() {
        downloader._validate();
      }).to.throw(dirname + ' is not an existing folder');
    });

    it('should throw an error if path ends in a directory that does not exist',
      function() {
      downloader.filepath = '/new/test/filepath/';
      storjStub.utils = {
        existsSync: sinon.stub()
      };
      storjStub.utils.existsSync.returns(false);

      expect(function() {
        downloader._validate();
      }).to.throw(downloader.filepath + ' is not an existing folder');
    });

    it('should not throw an error if the path is valid', function() {
      downloader.filepath = testOptions.filepath;
      storjStub.utils = {
        existsSync: sinon.stub()
      };
      storjStub.utils.existsSync.onCall(0).returns(false);
      storjStub.utils.existsSync.onCall(1).returns(true);

      expect(function() {
        downloader._validate();
      }).to.not.throw();
    });
  });

  describe('#_stripISOString', function() {
    before(function() {
      var oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
      downloader = new Downloader(testClient, testFileId, testBucketId,
        testOptions);
      Downloader.prototype._validate = oldValidate;
    });
    it('should remove the ISO portion of a string passed in', function() {
      var iso = (new Date().toISOString()).replace(/:/g, ';');
      var str = '(' + iso + ')-teststring';
      expect(downloader._stripISOString(str)).to.equal('teststring');
    });
  });

  describe('#_getInfo', function() {
    before(function() {
      sandbox = sinon.sandbox.create();
      var oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
      downloader = new Downloader(testClient, testFileId, testBucketId,
        testOptions);
      Downloader.prototype._validate = oldValidate;
      sandbox.spy(Downloader.prototype, '_stripISOString');
    });
    after(function() {
      sandbox.restore();
    });

    it('should return an error if the client returns one', function() {
      var errorMsg = 'This is an error';
      clientStub.getFileInfo = function(bucket, fileid, cb) {
        cb(new Error(errorMsg));
      };

      var cb = sinon.stub();
      downloader._getInfo(cb);

      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.not.equal(null);
      expect(err.message).to.contain(errorMsg);


    });

    it('should save the file metadata returned by the client', function() {
      var filename = 'testfilename';
      var mimetype = 'text/plain';
      var size = 2048;
      var id = 4524;
      clientStub.getFileInfo = function(bucket, fileid, cb) {
        var testFile = {
          filename: filename,
          mimetype: mimetype,
          size: size,
          id: id
        };
        cb(null, testFile);
      };

      var cb = sinon.stub();
      downloader._getInfo(cb);

      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.equal(null);
      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('info', sinon.match.any,
        [filename, mimetype, size, id])).to.equal(true);
      expect(downloader._stripISOString.calledWithMatch(filename))
        .to.equal(true);
      expect(downloader.fileMeta.filename).to.equal(filename);
      expect(downloader.fileMeta.mimetype).to.equal(mimetype);
      expect(downloader.fileMeta.size).to.equal(size);
      expect(downloader.fileMeta.id).to.equal(id);
    });
  });

  describe('#_determineSaveLocation', function() {
    before(function() {
      sandbox = sinon.sandbox.create();
      var oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
      downloader = new Downloader(testClient, testFileId, testBucketId,
        testOptions);
      Downloader.prototype._validate = oldValidate;
    });
    after(function() {
      sandbox.restore();
    });

    it('should return an error if the file does not exist', function() {
      downloader.fileMeta = null;

      var cb = sinon.stub();
      downloader._determineSaveLocation(cb);

      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.not.equal(null);
      expect(err.message).to.contain('file ' + testFileId +
        ' does not exist in bucket ' + testBucketId);
    });

    it('should return an error if filepath is a directory and ' +
      'filepath + filename already exists', function() {
      downloader.filepath = '/test/file/path/';
      downloader.fileMeta = {
        filename: 'testfilename.ext'
      };
      var fullpath = downloader.filepath + downloader.fileMeta.filename;

      storjStub.utils = {
        existsSync: sinon.stub().returns(true)
      };

      var cb = sinon.stub();
      downloader._determineSaveLocation(cb);

      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.not.equal(null);
      expect(err.message).to.contain('Refusing to overwrite file at ' +
        fullpath);
    });

    it('should properly set the destination if filepath is a directory and ' +
      'filepath + filename does not exist', function() {
      downloader.filepath = '/test/file/path/';
      downloader.fileMeta = {
        filename: 'testfilename.ext'
      };
      var fullpath = downloader.filepath + downloader.fileMeta.filename;

      storjStub.utils = {
        existsSync: sinon.stub()
      };
      storjStub.utils.existsSync.onCall(0).returns(true);
      storjStub.utils.existsSync.onCall(1).returns(false);

      var cb = sinon.stub();
      downloader._determineSaveLocation(cb);

      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.equal(null);
      expect(downloader.destination).to.equal(fullpath);
    });

    it('should properly set the destination if filepath is a file', function() {
      downloader.filepath = '/test/file/path.ext';
      downloader.fileMeta = {};

      storjStub.utils = {
        existsSync: sinon.stub().returns(false)
      };

      var cb = sinon.stub();
      downloader._determineSaveLocation(cb);

      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.equal(null);
      expect(downloader.destination).to.equal(downloader.filepath);
    });
  });

  describe('#_getKeyRing', function() {
    before(function() {
      sandbox = sinon.sandbox.create();
      var oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
      downloader = new Downloader(testClient, testFileId, testBucketId,
        testOptions);
      Downloader.prototype._validate = oldValidate;
    });
    after(function() {
      sandbox.restore();
    });

    it('should save the keyring returned by utils', function() {
      var testKeyRing = 'testkeyring';
      downloader.keypass = 'testkeypass';
      utilsStub.getKeyRing = function(keypass, cb) {
        cb(testKeyRing);
      };
      sinon.spy(utilsStub, 'getKeyRing');

      var cb = sinon.stub();
      downloader._getKeyRing(cb);

      expect(utilsStub.getKeyRing.callCount).to.equal(1);
      var pass = utilsStub.getKeyRing.getCall(0).args[0];
      expect(pass).to.equal(downloader.keypass);
      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.equal(null);
      expect(downloader.keyring).to.equal(testKeyRing);
    });
  });

  describe('#_createFileStream', function() {
    before(function() {
      sandbox = sinon.sandbox.create();
      var oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
      downloader = new Downloader(testClient, testFileId, testBucketId,
        testOptions);
      Downloader.prototype._validate = oldValidate;
    });
    after(function() {
      sandbox.restore();
    });

    it('should call the final callback when the download is complete',
      function() {
      var testDest = '/test/destination/file.ext';
      downloader.destination = testDest;
      var writableMock = stream.Writable();
      fsStub.createWriteStream = function() {
        return writableMock;
      };
      sinon.spy(fsStub, 'createWriteStream');
      clientStub.createFileStream = sinon.stub();

      var cb = sinon.stub();
      var finalCb = sinon.stub();
      downloader.finalCallback = finalCb;
      downloader._createFileStream(cb);
      writableMock.emit('finish');

      expect(fsStub.createWriteStream.callCount).to.equal(1);
      expect(fsStub.createWriteStream.calledWithMatch(testDest)).to.equal(true);
      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('info',
        'File downloaded and written to',
        [downloader.destination])).to.equal(true);
      expect(finalCb.callCount).to.equal(1);
      expect(finalCb.calledWithMatch(null, testDest)).to.equal(true);
    });

    it('should pass an error to the callback if there is an error with ' +
      'the write stream', function() {
      var testDest = '/test/destination/file.ext';
      downloader.destination = testDest;
      var writableMock = stream.Writable();
      fsStub.createWriteStream = function() {
        return writableMock;
      };
      sinon.spy(fsStub, 'createWriteStream');
      clientStub.createFileStream = sinon.stub();

      var cb = sinon.stub();
      downloader._createFileStream(cb);
      var errMsg = 'this is an error';
      writableMock.emit('error', new Error(errMsg));

      expect(fsStub.createWriteStream.callCount).to.equal(1);
      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.not.equal(null);
      expect(err.message).to.contain(errMsg);
    });

    it('should create a file stream from the client to be used ' +
      'in _handleFileStream', function() {
      var testDest = '/test/destination/file.ext';
      downloader.destination = testDest;
      var writableMock = stream.Writable();
      fsStub.createWriteStream = function() {
        return writableMock;
      };
      clientStub.createFileStream = sinon.stub();

      var cb = sinon.stub();
      downloader._createFileStream(cb);

      expect(clientStub.createFileStream.callCount).to.equal(1);
      var exclude = testOptions.env.exclude.split(',');
      var expectedArgs = [testBucketId, testFileId, {exclude: exclude}, cb];
      var actualArgs = clientStub.createFileStream.getCall(0).args;
      expect(expectedArgs).to.deep.equal(actualArgs);
    });
  });

  describe('#_handleFileStream', function() {
    before(function() {
      sandbox = sinon.sandbox.create();
      var oldValidate = Downloader.prototype._validate;
      Downloader.prototype._validate = sinon.stub();
      downloader = new Downloader(testClient, testFileId, testBucketId,
        testOptions);
      Downloader.prototype._validate = oldValidate;
    });
    after(function() {
      sandbox.restore();
    });

    it('should pass an error to the callback if no decryption key is found',
      function() {
      var errMsg = 'No decryption key found in key ring';
      var readableMock = stream.Readable();
      downloader.keyring = {
        get: sinon.stub().returns(null)
      };

      var cb = sinon.stub();
      downloader._handleFileStream(readableMock, cb);

      expect(downloader.keyring.get.callCount).to.equal(1);
      expect(downloader.keyring.get.calledWith(testFileId)).to.equal(true);
      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.not.equal(null);
      expect(err.message).to.contain(errMsg);
    });

    it('should pass an error to the callback if the download fails and the ' +
      'attempt to unlink the local file fails', function() {
      var errMsg = 'Failed to unlink partial file';
      var testSecret = 'test secret';
      var decryptStream = stream.Transform();
      var readableMock = stream.Readable();
      var writableMock = stream.Writable();
      var testDestination = '/test/destination';

      downloader.keyring = {
        get: sinon.stub().returns(testSecret)
      };
      storjStub.DecryptStream = sinon.stub().returns(decryptStream);
      downloader.target = writableMock;
      downloader.destination = testDestination;
      fsStub.unlink = function(destination, cb) {
        cb(true);
      };
      sinon.spy(fsStub, 'unlink');

      var cb = sinon.stub();
      downloader._handleFileStream(readableMock, cb);

      var readableErrMsg = 'test readable error message';
      readableMock.emit('error', new Error(readableErrMsg));

      expect(storjStub.DecryptStream.callCount).to.equal(1);
      expect(storjStub.DecryptStream.calledWith(testSecret)).to.equal(true);
      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('warn',
        'Failed to download shard', [readableErrMsg])).to.equal(true);
      expect(fsStub.unlink.callCount).to.equal(1);
      expect(fsStub.unlink.calledWith(testDestination)).to.equal(true);
      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.not.equal(null);
      expect(err.message).to.contain(errMsg);
    });

    it('should pass an error to the callback if the download fails and there ' +
      'is no pointer variable on the resulting error', function() {
      var errMsg = 'Failed to download file';
      var testSecret = 'test secret';
      var decryptStream = stream.Transform();
      var readableMock = stream.Readable();
      var writableMock = stream.Writable();
      var testDestination = '/test/destination';

      downloader.keyring = {
        get: sinon.stub().returns(testSecret)
      };
      storjStub.DecryptStream = sinon.stub().returns(decryptStream);
      downloader.target = writableMock;
      downloader.destination = testDestination;
      fsStub.unlink = function(destination, cb) {
        cb();
      };
      sinon.spy(fsStub, 'unlink');

      var cb = sinon.stub();
      downloader._handleFileStream(readableMock, cb);

      var readableErrMsg = 'test readable error message';
      readableMock.emit('error', new Error(readableErrMsg));

      expect(storjStub.DecryptStream.callCount).to.equal(1);
      expect(storjStub.DecryptStream.calledWith(testSecret)).to.equal(true);
      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('warn',
        'Failed to download shard', [readableErrMsg])).to.equal(true);
      expect(fsStub.unlink.callCount).to.equal(1);
      expect(fsStub.unlink.calledWith(testDestination)).to.equal(true);
      expect(cb.callCount).to.equal(1);
      var err = cb.getCall(0).args[0];
      expect(err).to.not.equal(null);
      expect(err.message).to.contain(errMsg);
    });

    it('should modify the exclusion list and retry the download if the ' +
      'download fails and there is a pointer variable on the resulting error',
      function() {
      var testSecret = 'test secret';
      var decryptStream = stream.Transform();
      var readableMock = stream.Readable();
      var writableMock = stream.Writable();
      var testDestination = '/test/destination';

      downloader.keyring = {
        get: sinon.stub().returns(testSecret)
      };
      storjStub.DecryptStream = sinon.stub().returns(decryptStream);
      downloader.target = writableMock;
      downloader.destination = testDestination;
      fsStub.unlink = function(destination, cb) {
        cb();
      };
      sinon.spy(fsStub, 'unlink');
      downloader.start = sinon.stub();
      downloader.finalCallback = sinon.stub();

      var cb = sinon.stub();
      downloader._handleFileStream(readableMock, cb);

      var readableErrMsg = 'test readable error message';
      var err = new Error(readableErrMsg);
      err.pointer = {
        farmer: {
          nodeID: 'e'
        }
      };
      readableMock.emit('error', err);

      expect(storjStub.DecryptStream.callCount).to.equal(1);
      expect(storjStub.DecryptStream.calledWith(testSecret)).to.equal(true);
      expect(LoggerStub.log.callCount).to.equal(2);
      expect(LoggerStub.log.calledWithMatch('warn',
        'Failed to download shard', [readableErrMsg])).to.equal(true);
      expect(LoggerStub.log.calledWithMatch('info',
        'Retrying download')).to.equal(true);
      expect(fsStub.unlink.callCount).to.equal(1);
      expect(fsStub.unlink.calledWith(testDestination)).to.equal(true);
      expect(downloader.exclude).to.equal(testOptions.env.exclude + ',e');
      expect(downloader.start.callCount).to.equal(1);
      expect(downloader.start.calledWith(downloader.finalCallback))
        .to.equal(true);
    });

    it('should log information about the download progress for each ' +
      'chunk downloaded', function(done) {
      var testSecret = 'test secret';
      var decryptStream = stream.PassThrough();
      var readableMock = stream.Readable({read: sinon.stub()});
      readableMock._length = 21;
      var writableMock = stream.Writable({write: sinon.stub().callsArg(2)});
      var testDestination = '/test/destination';

      downloader.keyring = {
        get: sinon.stub().returns(testSecret)
      };
      storjStub.DecryptStream = sinon.stub().returns(decryptStream);
      downloader.target = writableMock;
      downloader.destination = testDestination;

      var cb = sinon.stub();
      downloader._handleFileStream(readableMock, cb);

      readableMock.push('teststring');
      readableMock.push('teststring2');
      readableMock.push(null);

      downloader.target.on('finish', function() {
        expect(storjStub.DecryptStream.callCount).to.equal(1);
        expect(storjStub.DecryptStream.calledWith(testSecret)).to.equal(true);
        expect(LoggerStub.log.callCount).to.equal(2);
        expect(LoggerStub.log.calledWithMatch('info', 'Received %s of %s bytes',
          [10, 21])).to.equal(true);
        expect(LoggerStub.log.calledWithMatch('info', 'Received %s of %s bytes',
          [21, 21])).to.equal(true);
        done();
      });
    });

    it('should pipe the download through the decrypter and the ' +
      'file write stream', function(done) {
      var testSecret = 'test secret';
      var decryptStream = stream.PassThrough();
      var readableMock = stream.Readable({read: sinon.stub()});
      readableMock._length = 21;
      var writableMock = stream.Writable({write: sinon.stub().callsArg(2)});
      var testDestination = '/test/destination';

      downloader.keyring = {
        get: sinon.stub().returns(testSecret)
      };
      storjStub.DecryptStream = sinon.stub().returns(decryptStream);
      downloader.target = writableMock;
      downloader.destination = testDestination;

      var cb = sinon.stub();
      downloader._handleFileStream(readableMock, cb);

      readableMock.push('teststring');
      readableMock.push('teststring2');
      readableMock.push(null);

      downloader.target.on('finish', function() {
        done();
      });
    });
  });

  describe('#start', function() {

  });
});
