/**
 * @fileOverview azure-storage blob helper.
 * Performances: https://docs.microsoft.com/en-us/azure/storage/common/storage-scalability-targets.
 * 20,000 reqs per storage account per second.
 * 200 storage account per sub.
 * max 500 TB per storage axxount.
 * non-us max ingress 5Gbps for GRS/ZRS, 10Gps for LRS.
 * non-us max egress 10Gps for RA-GRS/GRS/ZRS, 15GPS for LRS.
 *
 * @author       <a href="mailto:sjian@microsoft.com">Arthur Jiang</a>
 * @version      1.0.0
 *
 * @requires     azure-storage,async
 * @requires     ./log,../config
 * @requires     AZURE_STORAGE_ACCOUNT,AZURE_STORAGE_ACCESS_KEY
 * @module       blob
 */

const async = require('async');
const config = require(`${__dirname}/../config`);
const azure = require('azure-storage');
const blobSvc = azure.createBlobService(config.secret.azure.storage[0].connection_string == 'get_it_from_portal' || config.secret.azure.storage[0].connection_string == undefined ? process.env.AZURE_STORAGE_CONNECTION_STRING : config.secret.azure.storage[0].connection_string);

const Log = require(`${__dirname}/log`);

const log = new Log({
  tag: 'blob'
});

const blobUnit = 4 * 1024 * 1024;
const maxBlobSize = 5000 * blobUnit;

/**
 * Append string to blob - internal use an append blob, the string length must less than 5000 * 4 * 1024 * 1024 bytes. If the append blob does
 * not exist, will create a new one; if the append blob does exist, will only append at the end of it.
 * Naming rules for container and blob: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata.
 * @param {string} container - container name
 * @param {string} blob - blob name
 * @param {string} text - input string
 * @param {function} cb - call back
 * @example
 * // append text
 * blob.appendText('testcontainer', 'appendtext', 'big string1', (err, res) => {
 *   ...
 * });
 * blob.appendText('testcontainer', 'appendtext', 'big string2', (err, res) => {
 *   ...
 * })
 */
const appendText = (container, blob, text, cb) => {
  const createContainer = (cb) => {
    blobSvc.createContainerIfNotExists(container, (err, result, response) => {
      cb(err, response);
    });
  };

  const createBlob = (cb) => {
    let options = {
      contentSettings: {
        contentType: 'text/plain;charset=utf-8'
      }
    };

    if (text.length > maxBlobSize) {
      cb(`current text size is ${text.length/1000000}MB, blob size should be less than ${maxBlobsize/1000}MB`);
    } else {
      blobSvc.doesBlobExist(container, blob, (err, res) => {
        if (!err && res['exists'] == true) {
          log.info(`doesBlobExist:`, res);
          cb(null);
        } else {
          log.info(`doesBlobExist:`, err, res);
          blobSvc.createAppendBlobFromText(container, blob, '', options, (err, result, response) => {
            cb(err, response);
          });
        }
      });
    }
  };

  const appendSlicedText = (cb) => {
    let slicedText = [];

    for (let i = 0; i < Math.ceil(text.length/blobUnit); i++) {
      slicedText.push(text.slice(i * blobUnit, (i + 1) * blobUnit));
      log.info(`Slice ${i}: ${slicedText[i].length} bytes`);
    }

    async.eachSeries(slicedText, (text, cb) => {
      blobSvc.appendFromText(container, blob, text, (err, result, response) => {
        cb(err, response);
      });
    }, (err) => {
      cb(err);
    });
  };

  const createSAS = (cb) => {
    let startDate = new Date();
    let expiryDate = new Date(startDate);
    expiryDate.setMinutes(startDate.getMinutes() + 512640);
    startDate.setMinutes(startDate.getMinutes() - 1440);

    let sharedAccessPolicy = {
      AccessPolicy: {
        Permissions: azure.BlobUtilities.SharedAccessPermissions.READ,
        Start: startDate,
        Expiry: expiryDate
      }
    };

    let blobSAS = blobSvc.generateSharedAccessSignature(container, blob, sharedAccessPolicy);
    let host = blobSvc.host;
    let sasUrl = `${host.primaryHost}${container}/${blob}?${blobSAS}`;

    cb(null, sasUrl);
  };

  async.series([createContainer, createBlob, appendSlicedText, createSAS], (err, results) => {
    log.info(`Blob async series:`, err, results);

    if (!err) {
      cb(null, results[3]);
    } else {
      cb(err);
    }
  });
};

/**
 * Write short string to blob - internal use an block blob, the string length must less than 64 * 1024 * 1024 bytes.
 * Always create a new block blob, if the name is same, will replace the old one.
 * Naming rules for container and blob: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata.
 * @param {string} container - container name
 * @param {string} blob - blob name
 * @param {string} text - input string
 * @param {object} options - <optional> blob/request options
 * @param {function} cb - call back
 * @example
 * // write text
 * blob.writeText('testcontainer', 'smalltext', 'small text', (err, res) => {
 *   if (!err) {
 *     // res is the blob url with token
 *     console.log(res);
 *   }
 * });
 */
const writeText = function (container, blob, text, options, cb) {
  if (arguments.length == 4) {
    cb = arguments[3];
    options = {
      contentSettings: {
        contentType: 'text/plain;charset=utf-8'
      }
    };
  }

  const createContainer = (cb) => {
    blobSvc.createContainerIfNotExists(container, (err, result, response) => {
      cb(err, response);
    });
  };

  const createBlob = (cb) => {
    blobSvc.createBlockBlobFromText(container, blob, text, options, (err, result, response) => {
      cb(err, response);
    });
  };

  const createSAS = (cb) => {
    let startDate = new Date();
    let expiryDate = new Date(startDate);
    expiryDate.setMinutes(startDate.getMinutes() + 512640);
    startDate.setMinutes(startDate.getMinutes() - 1440);

    let sharedAccessPolicy = {
      AccessPolicy: {
        Permissions: azure.BlobUtilities.SharedAccessPermissions.READ,
        Start: startDate,
        Expiry: expiryDate
      }
    };

    let blobSAS = blobSvc.generateSharedAccessSignature(container, blob, sharedAccessPolicy);
    let host = blobSvc.host;
    let sasUrl = `${host.primaryHost}${container}/${blob}?${blobSAS}`;

    cb(null, sasUrl);
  };

  async.series([createContainer, createBlob, createSAS], (err, results) => {
    log.info(`Blob async series:`, err, results);

    if (!err) {
      cb(null, results[2]);
    }else {
      cb(err);
    }
  });
};

/**
 * Write stream to blob - internal use an block blob. Always create a new block blob, if the name is same, will replace the old one.
 * 50MB/s, sample: http://willi.am/blog/2014/07/03/azure-blob-storage-and-node-downloading-blobs.
 * Naming rules for container and blob: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata.
 * @param {string} container - container name
 * @param {string} blob - blob name
 * @param {stream} stream - read stream
 * @param {object} options - <optional> blob/request options
 * @param {function} cb - call back
 * @example
 * // write stream
 * let readStream = fs.createReadStream(`${your_file_path}`);
 * blob.writeStream('testcontainer', 'teststream', readStream, (err, res) => {
 *   if (!err) {
 *     // res is the blob url with token
 *     console.log(res);
 *   }
 * });
 */
const writeStream = function (container, blob, stream, options, cb) {
  if (arguments.length == 4) {
    cb = arguments[3];
    options = {
      contentSettings: {
        contentType: 'text/plain;charset=utf-8'
      }
    };
  }

  const createContainer = (cb) => {
    blobSvc.createContainerIfNotExists(container, (err, result, response) => {
      cb(err, response);
    });
  };

  const createBlob = (cb) => {
    let blobStream = blobSvc.createWriteStreamToBlockBlob(container, blob, options, (err, result, response) => {
      if (err) {
        log.info(`Stream upload err: ${err}`);
      } else {
        log.info(`Stream upload finished: ${result}`);
      }

      cb(err, result);
    });

    stream.pipe(blobStream);
  };

  const createSAS = (cb) => {
    let startDate = new Date();
    let expiryDate = new Date(startDate);
    expiryDate.setMinutes(startDate.getMinutes() + 525600);
    startDate.setMinutes(startDate.getMinutes() - 1440);

    let sharedAccessPolicy = {
      AccessPolicy: {
        Permissions: azure.BlobUtilities.SharedAccessPermissions.READ,
        Start: startDate,
        Expiry: expiryDate
      }
    };

    let blobSAS = blobSvc.generateSharedAccessSignature(container, blob, sharedAccessPolicy);
    let host = blobSvc.host;
    let sasUrl = `${host.primaryHost}${container}/${blob}?${blobSAS}`;

    cb(null, sasUrl);
  };

  async.series([createContainer, createBlob, createSAS], (err, results) => {
    log.info(`Blob async series:`, err, results);

    if (!err) {
      cb(null, results[2]);
    } else {
      cb(err);
    }
  });
};

/**
 * Read string from blob.
 * Naming rules for container and blob: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata.
 * @param {string} container - container name
 * @param {string} blob - blob name
 * @param {function} cb - call back
 * @example
 * // read text
 * blob.readText('testcontainer', 'smalltext'), (err, res) => {
 *   ...
 * });
 */
const readText = (container, blob, cb) => {
  blobSvc.getBlobToText(container, blob, (err, text) => {
    cb(err, text);
  });
};

/**
 * Read stream from blob.
 * Naming rules for container and blob: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata.
 * @param {string} container - container name
 * @param {string} blob - blob name
 * @param {stream} stream - write stream
 * @param {function} cb - call back
 * @example
 * // read stream
 * let writeStream = fs.createWriteStream(`${your_file_path}`);
 * blob.readStream('testcontainer', 'teststream', writeStream, (err, res) => {
 *   ...
 *   }
 * })
 */
const readStream = (container, blob, stream, cb) => {
  blobSvc.getBlobToStream(container, blob, stream, (err, res) => {
    cb(err, res);
  });
};

/**
 * Delete blob
 * @param {string} container
 * @param {string} blob
 * @param {function} cb
 */
const deleteBlob = (container, blob, cb) => {
  blobSvc.deleteBlob(container, blob, (err, res) => {
    cb(err, res);
  });
};

const setMeta = (container, blob, meta, cb) => {
  blobSvc.setBlobMetadata(container, blob, meta, (err, res) => {
    cb(err, res);
  });
};

module.exports = {
  writeText: writeText,
  appendText: appendText,
  writeStream: writeStream,
  readText: readText,
  readStream: readStream,
  deleteBlob: deleteBlob,
  setMeta: setMeta
};
