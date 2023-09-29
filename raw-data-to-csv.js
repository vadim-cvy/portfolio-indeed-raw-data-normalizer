const fs = require('fs')
const readline = require('readline')
const fastcsv = require('fast-csv')

const RAW_DIR_PATH = './data/raw/'
const NORMALIZED_FILE_PATH = './data/normalized/normalized.csv'

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

const readJobs = async (filePath, onJobCb, onComplete) => {
  return new Promise( resolve => {
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
        console.error(`Error parsing line ${filePath}:${lineNumber}`)
        return
      }

      onJobCb(job)
    })

    readInterface.on('close', resolve)

    readInterface.on('error', err => {
      throw new Error(err)
    })
  })
}

const getAllPresentedFields = async filePath => {
  return new Promise(resolve => {
    const fields = new Set();

    readJobs(filePath, job =>
      Object.keys(job).forEach(field => fields.add(field))
    )
    .then( () => resolve( fields ) )
  })
}

const readdir = dir => {
  return new Promise((resolve, reject) => {
    fs.readdir(dir, (err, filePathes) => {
      if (err) reject(err);
      else resolve(filePathes.map(filePath => RAW_DIR_PATH + filePath ));
    });
  });
}





readdir(RAW_DIR_PATH).then(async filePathes => {
  const handledJobsIds = new Set()

  const allPresentedFields = new Set()

  for (let filePath of filePathes) {
    (await getAllPresentedFields(filePath)).forEach( fieldName => allPresentedFields.add( fieldName ) )
  }

  const csvStream = fastcsv.format({ headers: true, writeHeaders: true });
  const writeStream = fs.createWriteStream(NORMALIZED_FILE_PATH)
    .on('error', err => console.error('Error writing to file:', err));

  csvStream.pipe(writeStream);

  for (let filePath of filePathes) {
    await readJobs(filePath, job => {
      if (handledJobsIds.has(job.uniq_id)) {
        console.log( 'Duplicate job: ' + job.uniq_id )
        return;
      }

      handledJobsIds.add(job.uniq_id);

      const normalizedJob = normalizeJob(job, allPresentedFields);

      csvStream.write(normalizedJob);
    });

    console.log(`"${filePath}" - ready!`)
  }

  csvStream.end();

  console.log( `Total unique jobs: ${handledJobsIds.size}` )
}).catch(err => {
  console.error("Error reading the directory:", err);
});