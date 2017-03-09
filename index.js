var url = require('url')
var crypto = require('crypto')
var fs = require('fs')
var ndjson = require('ndjson')
var transform = require('parallel-transform')
var through = require('through2')
var JSFtp = require("jsftp")
var blobStore = require('content-addressable-blob-store')
var pump = require('pump')
var dataDir = process.argv[2]
if (!dataDir) throw new Error('must specify data directory')
var blobs = blobStore(dataDir)
var PARALLEL = 1000
var seen = {}

function getResponse (item, cb) {
  var start = Date.now()
  var parsed = url.parse(item.url)
  
  var ftp = new JSFtp({host: parsed.hostname})
  ftp.list(parsed.path, function (err, res) {
    if (err) return error(err)
    var end = Date.now()
    var elapsed = end - start
    var meta = (file) => ({ url: item.url, date: new Date(), took: elapsed, package_id: item.package_id, id: item.id, file })
    res.split('\n').forEach((file) => cb(null, meta(file)))
  })

  function error (err) {
    var obj = {url: item.url, date: new Date(), package_id: item.package_id, id: item.id, error: error}
    cb(null, obj)
  }
}

process.stdin
  .pipe(ndjson.parse())
  .pipe(through.obj(function (item, enc, next) {
    var self = this
    if (item.url.slice(0, 4) !== 'ftp:') return next()
    var hash = crypto.createHash('sha256').update(item.url).digest('hex')
    if (seen[hash]) return
    self.push(item)
    seen[hash] = true
    next()

  }))
  .pipe(transform(PARALLEL, getResponse))
  .pipe(ndjson.serialize())
  .pipe(process.stdout)
