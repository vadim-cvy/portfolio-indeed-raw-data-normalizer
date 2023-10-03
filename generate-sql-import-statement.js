const fs = require('fs')
const readline = require('readline')
var path = require("path");

const DATASET_FILE_PATH = path.resolve('./dataset/normalized/normalized.csv')

const DB_TABLE_NAME = 'jobs'

const getColumnNames = async () => {
  const fileStream = fs.createReadStream(DATASET_FILE_PATH)

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  })

  let headingLine

  for await (const line of rl) {
    headingLine = line
    break
  }

  return headingLine.split( ',' )
}

getColumnNames().then( columnNames =>
{
  let sql = `CREATE TABLE ${DB_TABLE_NAME} (\n`

  sql += columnNames
    .map( columnName => `  ${columnName} TEXT` )
    .join( ',\n' )

  sql += ',\n'

  sql +=
    '  id INT NOT NULL AUTO_INCREMENT,\n' +
    '  PRIMARY KEY (id)\n' +
    ');\n\n'

  sql +=
    `LOAD DATA INFILE "${DATASET_FILE_PATH}" INTO TABLE ${DB_TABLE_NAME} ` +
    'FIELDS TERMINATED BY "," ENCLOSED BY "\\"" LINES TERMINATED BY "\\n" ' +
    'IGNORE 1 ROWS ' +
    `(${columnNames.join( ',' )});` // csv lacks id column so presented columns must be specified

  console.log( sql )
})