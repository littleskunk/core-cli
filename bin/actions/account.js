'use strict';
var storj = require('storj-lib');
var log = require('./../logger')().log;
var utils = require('./../utils');
var fs = require('fs');

module.exports.getInfo =  function() {
  var client = this._storj.PublicClient();

  client.getInfo(function(err, info) {
     if (err) {
       return log('error', err.message);
     }

     log('info', 'Title:             %s', [info.info.title]);
     log('info', 'Description:       %s', [info.info.description]);
     log('info', 'Version:           %s', [info.info.version]);
     log('info', 'Host:              %s', [info.host]);
  });
};

module.exports.register = function() {
  var client = this._storj.PublicClient();

  utils.getCredentials(function(err, result) {
    if (err) {
      return log('error', err.message);
    }

    client.createUser({
      email: result.email,
      password: result.password
    }, function(err) {
      if (err) {
        return log('error', err.message);
      }

      log('info', 'Registered! Check your email to activate your account.');
    });
  });
};

module.exports.login = function() {
  var self = this;

  if (storj.utils.existsSync(self._storj.keypath())) {
    return log('error', 'This device is already paired.');
  }

  utils.getCredentials(function(err, result) {
    if (err) {
      return log('error', err.message);
    }

    var client = storj.BridgeClient(self._storj.getURL(), {
      basicauth: result
    });
    var keypair = storj.KeyPair();

    client.addPublicKey(keypair.getPublicKey(), function(err) {
      if (err) {
        return log('error', err.message);
      }

      fs.writeFileSync(self._storj.keypath(), keypair.getPrivateKey());
      log('info', 'This device has been successfully paired.');
    });
  });
};

module.exports.logout = function() {
  var self = this;
  var client = this._storj.PrivateClient();
  var keypair = this._storj.loadKeyPair();

  client.destroyPublicKey(keypair.getPublicKey(), function(err) {
    if (err) {
      log('info', 'This device has been successfully unpaired.');
      log('warn', 'Failed to revoke key, you may need to do it manually.');
      log('warn', 'Reason: ' + err.message);
      return fs.unlinkSync(self._storj.keypath());
    }

    fs.unlinkSync(self._storj.keypath());
    log('info', 'This device has been successfully unpaired.');
  });
};

module.exports.resetpassword = function(email) {
  var client = this._storj.PrivateClient();

  utils.getNewPassword(
    'Enter your new desired password',
    function(err, result) {
      client.resetPassword({
        email: email,
        password: result.password
      }, function(err) {
        if (err) {
          return log('error', 'Failed to request password reset, reason: %s',[
            err.message
          ]);
        }

        log(
          'info',
          'Password reset request processed, check your email to continue.'
        );
      }
    );
  });
};
