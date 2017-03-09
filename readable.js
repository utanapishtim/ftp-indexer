var ndjson = require('ndjson')
var Ftp = require('jsftp')
var i = 0
var names = ['permissions', 'numLinks', 'ownerName', 'ownerGroup', 'size', 'lastMod', 'name']
const logger = ndjson.serialize().pipe(process.stdout)
function ftpList(host, dir, ftp) {
  var ftp = ftp || new Ftp({ host })
  ftp.list(dir, (err, res) => {
    if (err) throw err
    res.split('\n')
      .map(
        (line) => line.split(' ')
          .filter(Boolean)
          .map((x, i, a) => (i === a.length - 1) ? x.slice(0, x.length - 1) : x)
          .reduce((last, next, i, array) => {
            if (i > 5 && i <= 7) return last;
            (i === 5)
              ? last.push(`${array[i]} ${array[i + 1]} ${array[i + 2]}`)
              : last.push(next)
            return last
          }, [])
          .reduce((obj, val, i) => {
            obj[names[i]] = val
            return obj
          }, {})
      ).forEach((data) => {
        if (data && Object.getOwnPropertyNames(data).length > 0) {
          try {
            logger.write(JSON.stringify({ host, dir, data }))
          } catch (e) {
            throw e
          }
          setTimeout(() => ftpList(host, `${dir}/${String(data.name)}`, ftp), i++ * 1500)
        }
      })
  })
}
ftpList('rockyftp.cr.usgs.gov', '.')
