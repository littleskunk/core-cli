'use strict';
var expect = require('chai').expect;
var proxyquire = require('proxyquire');
var sinon = require('sinon');

var LoggerStub = {
  log: sinon.stub()
};
var utilsStub = {};

var Buckets = proxyquire('../../bin/actions/buckets.js', {
  './../logger': function() {
    return LoggerStub;
  },
  './../utils': utilsStub
});

describe('buckets', function() {
  beforeEach(function() {
    LoggerStub.log.reset();
    for (var k in utilsStub) {
      try {
        delete utilsStub[k];
      } catch(e) {
        // occurs when a key in the object is not writable
      }
    }
  });

  describe('#list', function() {
    it('should log an error if the client responds with one', function() {
      var error = {
        message: 'This is an error.'
      };
      var PrivateClientStub = {
        getBuckets: function(cb) {
          cb(error);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub)
      };

      Buckets.list();

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
    });

    it('should warn if the client does not have any buckets', function() {
      var bucketList = [];
      var PrivateClientStub = {
        getBuckets: function(cb) {
          cb(null, bucketList);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub)
      };

      Buckets.list();

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('warn',
        'You have not created any buckets.')).to.equal(true);
    });

    it('should print the information for each bucket stored', function() {
      var bucketList = [
        {id: 1, name: 'testname1', storage: 43, transfer: 10},
        {id: 2, name: 'testname2', storage: 52, transfer: 20},
        {id: 3, name: 'testname3', storage: 11, transfer: 30}
      ];
      var PrivateClientStub = {
        getBuckets: function(cb) {
          cb(null, bucketList);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub)
      };

      Buckets.list();

      expect(LoggerStub.log.callCount).to.equal(bucketList.length);
      bucketList.forEach(function(testBucket) {
        var attributeList = [testBucket.id, testBucket.name,
          testBucket.storage, testBucket.transfer];
        expect(LoggerStub.log.calledWithMatch('info', 'ID:',
          attributeList)).to.equal(true);
      });
    });
  });

  describe('#get', function() {
    it('should log an error if the client responds with one', function() {
      var error = {
        message: 'This is an error.'
      };
      var PrivateClientStub = {
        getBucketById: function(id, cb) {
          cb(error);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.get(1);

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
    });

    it('should log info about the bucket with the id passed in', function() {
      var bucketList = [
        {id: 1, name: 'testname1', storage: 43, transfer: 10},
        {id: 2, name: 'testname2', storage: 52, transfer: 20},
        {id: 3, name: 'testname3', storage: 11, transfer: 30}
      ];
      var PrivateClientStub = {
        getBucketById: function(id, cb) {
          cb(null, bucketList[0]);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.get(1);

      var testBucket = bucketList[0];
      var attributeList = [testBucket.id, testBucket.name,
        testBucket.storage, testBucket.transfer];
      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('info', 'ID:',
        attributeList)).to.equal(true);
    });
  });

  describe('#remove', function() {
    it('should prompt the user if a force flag is not passed in', function() {
      utilsStub.getConfirmation = sinon.stub();
      var PrivateClientStub = {
        destroyBucketById: function() {
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.remove(1, {force: false});

      expect(utilsStub.getConfirmation.callCount).to.equal(1);
      expect(utilsStub.getConfirmation.calledWithMatch(
        'Are you sure you want to destroy this bucket?', sinon.match.any));
    });

    it('should log an error if the client responds with one', function() {
      var error = {
        message: 'This is an error.'
      };
      var PrivateClientStub = {
        destroyBucketById: function(id, cb) {
          cb(error);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.remove(1, {force: true});

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
    });

    it('should log a success message if the bucket is successfully destroyed',
      function() {
      var PrivateClientStub = {
        destroyBucketById: function(id, cb) {
          cb();
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.remove(1, {force: true});

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('info',
        'Bucket successfully destroyed.')).to.equal(true);
    });
  });

  describe('#add', function() {
    it('should log an error if the client responds with one', function() {
      var error = {
        message: 'This is an error.'
      };
      var testInfo = {
        name: 'test name',
        storage: 10,
        transfer: 15
      };
      var PrivateClientStub = {
        createBucket: function(info, cb) {
          cb(error);
        }
      };
      var createBucketSpy = sinon.spy(PrivateClientStub, 'createBucket');
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub)
      };

      Buckets.add(testInfo.name, testInfo.storage, testInfo.transfer);

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
      expect(createBucketSpy.calledWithMatch(testInfo,
        sinon.match.any)).to.equal(true);
    });

    it('should log info about the new bucket when it is added', function() {
      var testInfo = {
        name: 'test name',
        storage: 10,
        transfer: 15
      };
      var testId = 5;
      var PrivateClientStub = {
        createBucket: function(info, cb) {
          info.id = testId;
          cb(null, info);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub)
      };

      Buckets.add(testInfo.name, testInfo.storage, testInfo.transfer);

      var newBucketInfo = [
        testId, testInfo.name, testInfo.storage, testInfo.transfer
      ];
      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('info', 'ID:',
        newBucketInfo)).to.equal(true);
    });
  });

  describe('#update', function() {
    it('should log an error if the client responds with one', function() {
      var error = {
        message: 'This is an error.'
      };
      var testInfo = {
        name: 'test name',
        storage: 10,
        transfer: 15
      };
      var testId = 5;
      var PrivateClientStub = {
        updateBucketById: function(id, info, cb) {
          cb(error);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.update(testId, testInfo.name, testInfo.storage,
        testInfo.transfer);

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
    });

    it('should log info about the bucket if the bucket is successfully updated',
      function() {
      var testInfo = {
        name: 'test name',
        storage: 10,
        transfer: 15
      };
      var testId = 5;
      var PrivateClientStub = {
        updateBucketById: function(id, info, cb) {
          var updatedBucket = {
            id: id,
            name: info.name,
            storage: info.storage,
            transfer: info.transfer
          };
          cb(null, updatedBucket);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.update(testId, testInfo.name, testInfo.storage,
        testInfo.transfer);

      var updatedBucketInfo = [
        testId, testInfo.name, testInfo.storage, testInfo.transfer
      ];
      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('info', 'ID:',
        updatedBucketInfo)).to.equal(true);
    });
  });

  describe('#createtoken', function() {
    it('should log an error if the client responds with one', function() {
      var error = {
        message: 'This is an error.'
      };
      var testId = 5;
      var PrivateClientStub = {
        createToken: function(bucket, operation, cb) {
          cb(error);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.createtoken(testId, 'pull');

      expect(LoggerStub.log.callCount).to.equal(1);
      expect(LoggerStub.log.calledWithMatch('error',
        error.message)).to.equal(true);
    });

    it('should log info about the token if it is successfully created',
      function() {
      var testId = 5;
      var testToken = 10;
      var PrivateClientStub = {
        createToken: function(bucket, operation, cb) {
          var newToken = {
            token: testToken,
            bucket: bucket,
            operation: operation
          };
          cb(null, newToken);
        }
      };
      Buckets._storj = {
        PrivateClient: sinon.stub().returns(PrivateClientStub),
        getRealBucketId: sinon.stub().returnsArg(0)
      };

      Buckets.createtoken(testId, 'pull');

      var newTokenInfo = [testToken, testId, 'pull'];
      expect(LoggerStub.log.callCount).to.equal(2);
      expect(LoggerStub.log.calledWithMatch('info',
        'Token successfully created.')).to.equal(true);
      expect(LoggerStub.log.calledWithMatch('info','Token:',
        newTokenInfo)).to.equal(true);
    });
  });
});
