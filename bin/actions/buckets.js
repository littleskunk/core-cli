'use strict';
var log = require('./../logger')().log;
var utils = require('./../utils');

module.exports.list = function() {
  var client = this._storj.PrivateClient();

  client.getBuckets(function(err, buckets) {
    if (err) {
      return log('error', err.message);
    }

    if (!buckets.length) {
      return log('warn', 'You have not created any buckets.');
    }

    buckets.forEach(function(bucket) {
      log(
        'info',
        'ID: %s, Name: %s, Storage: %s, Transfer: %s',
        [bucket.id, bucket.name, bucket.storage, bucket.transfer]
      );
    });
  });
};

module.exports.get = function(id) {
  var client = this._storj.PrivateClient();
  id = this._storj.getRealBucketId(id);

  client.getBucketById(id, function(err, bucket) {
    if (err) {
      return log('error', err.message);
    }

    log(
      'info',
      'ID: %s, Name: %s, Storage: %s, Transfer: %s',
      [bucket.id, bucket.name, bucket.storage, bucket.transfer]
    );
  });
};

module.exports.remove = function(id, env) {
  var client = this._storj.PrivateClient();
  id = this._storj.getRealBucketId(id);

  function destroyBucket() {
    client.destroyBucketById(id, function(err) {
      if (err) {
        return log('error', err.message);
      }

      log('info', 'Bucket successfully destroyed.');
    });
  }

  if (!env.force) {
    return utils.getConfirmation(
      'Are you sure you want to destroy this bucket?',
      destroyBucket
    );
  }

  destroyBucket();
};

module.exports.add = function(name, storage, transfer) {
  var client = this._storj.PrivateClient();

  client.createBucket({
    name: name,
    storage: storage,
    transfer: transfer
  }, function(err, bucket) {
    if (err) {
      return log('error', err.message);
    }

    log(
      'info',
      'ID: %s, Name: %s, Storage: %s, Transfer: %s',
      [bucket.id, bucket.name, bucket.storage, bucket.transfer]
    );
  });
};

module.exports.update = function(id, name, storage, transfer) {
  var client = this._storj.PrivateClient();
  id = this._storj.getRealBucketId(id);

  client.updateBucketById(id, {
    name: name,
    storage: storage,
    transfer: transfer
  }, function(err, bucket) {
    if (err) {
      return log('error', err.message);
    }

    log(
      'info',
      'ID: %s, Name: %s, Storage: %s, Transfer: %s',
      [bucket.id, bucket.name, bucket.storage, bucket.transfer]
    );
  });
};

module.exports.makePublic = function(id, env) {
  var publicPush = env.push ? true : false;
  var publicPull = env.pull ? true : false;

  var client = this._storj.PrivateClient();
  id = this._storj.getRealBucketId(id);

  var _finish = function(permissions, bucketKey) {
    client.updateBucketById(id, {
      publicPermissions: permissions,
      encryptionKey: bucketKey
    }, function(err, bucket) {
      if (err) {
        return log('error', err.message);
      }
      var updatedPull = bucket.publicPermissions.includes('PULL');
      var updatedPush = bucket.publicPermissions.includes('PUSH');
      var key = bucket.encryptionKey.length === 0 ? null : bucket.encryptionKey;
      log(
        'info',
        'ID: %s, Name: %s, Public Pull: %s, Public Push: %s, Key: %s',
        [bucket.id, bucket.name, updatedPull, updatedPush, key]
      );
    });
  };

  var permissions = [];
  if (publicPull) {
    permissions.push('PULL');
  }
  if (publicPush) {
    permissions.push('PUSH');
  }

  if (permissions.length === 0) {
    return _finish([], '');
  }

  var keypass = this._storj.getKeyPass();
  utils.getKeyRing(keypass, function(keyring) {
    var bucketKey = keyring.generateBucketKey(id);
    if (bucketKey === null) {
      return log('error', 'You must generate a deterministic seed');
    }
    _finish(permissions, bucketKey);
  });

};

module.exports.createtoken = function(bucket, operation) {
  var client = this._storj.PrivateClient();
  bucket = this._storj.getRealBucketId(bucket);

  client.createToken(bucket, operation, function(err, token) {
    if (err) {
      return log('error', err.message);
    }

    log('info', 'Token successfully created.');
    log(
      'info',
      'Token: %s, Bucket: %s, Operation: %s',
      [token.token, token.bucket, token.operation]
    );
  });
};
