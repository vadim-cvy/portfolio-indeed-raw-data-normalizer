const fs = require('fs')
const path = require('path')
const readline = require('readline')
const fastcsv = require('fast-csv')

const RAW_DIR_PATH = './data/raw/'
const NORMALIZED_DIR_PATH = './data/normalized/'

const normalizeJob = (job, allPresentedFields) => {
  // Populate missed fields
  allPresentedFields.forEach(fieldName => {
    if (!job.hasOwnProperty(fieldName)) {
      job[fieldName] = ''
    }
  })

  const ignoreFields = [
    'uniq_id',
    'crawl_timestamp',
    'url',
    'inferred_iso2_lang_code',
    'predicted_language',
    'inferred_iso3_lang_code',
    'has_expired',
    'last_expiry_check_date',
    'latest_expiry_check_date',
    'dataset',
    'postdate_in_indexname_format',
    'job_desc_char_count',
    'ijp_is_prod_ready',
    'ijp_is_production_ready',
    'job_board',
    'apply_url',
    'domain',
    'segment_name',
  ]

  ignoreFields.forEach(fieldName => delete job[fieldName])

  if ( job.html_job_description.length > job.job_description.length )
  {
    job.job_description = job.html_job_description
  }

  delete job.html_job_description

  return job
}

const readJobs = (filePath, onJobCb, onComplete) => {
  const readInterface = readline.createInterface({
    input: fs.createReadStream(filePath),
    console: false
  })

  let lineNumber = 0

  readInterface.on('line', line => {
    lineNumber++
    let job
    try {
      job = JSON.parse(line)
    } catch (error) {
      console.error(`Error parsing line ${lineNumber}:`, error)
      return
    }

    onJobCb(job)
  })

  readInterface.on('close', onComplete)
  readInterface.on('error', err => {
    throw new Error(err)
  })
}

const getAllPresentedFields = filePath => {
  return new Promise(resolve => {
    const fields = new Set()

    readJobs(filePath,
      job => Object.keys(job).forEach(field => fields.add(field)),
      () => resolve(fields)
    )
  })
}

fs.readdir(RAW_DIR_PATH, (err, files) => {
  if (err) {
    console.error("Error reading the directory:", err)
    return
  }

  files.forEach(file => {
    const sourceFilePath = path.join(RAW_DIR_PATH, file)
    const destinationFilePath = path.join(NORMALIZED_DIR_PATH, file) + '.csv'

    getAllPresentedFields(sourceFilePath).then(allPresentedFields => {
      let isFirstLine = true

      const csvStream = fastcsv.format({ headers: true, writeHeaders: isFirstLine })
      const writeStream = fs.createWriteStream(destinationFilePath)
        .on('error', err => console.error('Error writing to file:', err))

      csvStream.pipe(writeStream)

      readJobs(sourceFilePath,
        job => {
          const normalizedJob = normalizeJob(job, allPresentedFields)
          csvStream.write(normalizedJob)

          if (isFirstLine) isFirstLine = false
        },
        () => {
          csvStream.end()
          console.log(`"${sourceFilePath}" - ready!`)
        }
      )
    }).catch(error => {
      console.error('Error processing file:', sourceFilePath, error)
    })
  })
})