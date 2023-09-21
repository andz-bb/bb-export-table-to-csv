import dotenv from "dotenv";
import fetch from "node-fetch";
import fs from "fs";
import readline from "readline";
import { SingleBar } from "cli-progress";
import Papa from "papaparse";

dotenv.config();

const progressBar = new SingleBar({
  format: "Uploading rows... | {bar} | {percentage}% | {value}/{total} Rows",
  barCompleteChar: "\u2588",
  barIncompleteChar: "\u2591",
  hideCursor: true,
});

const debugRow = (row) => {
  Object.keys(row).forEach((key) => {
    console.log(`Key: ${key}, Type: ${typeof row[key]}, Value: ${row[key]}`);
  });
};

const saveToJSON = (content, filename) => {
  fs.writeFileSync(filename, content);
  console.log(`JSON file saved as ${filename}`);
  process.exit(0);
};

const uploadRow = async (url, row) => {
  // debugRow(row);
  delete row._id;
  delete row._rev;
  // console.log("row ", row, JSON.stringify(row));

  // saveToJSON(JSON.stringify(row), "log.json");

  const options = {
    method: "POST",
    headers: {
      accept: "application/json",
      "x-budibase-app-id": process.env.APP_ID,
      "content-type": "application/json",
      "x-budibase-api-key": process.env.BUDIBASE_API_KEY,
    },
    body: JSON.stringify(row),
  };

  const response = await fetch(url, options);
  console.log(response);
};

const countRowsInCSV = async (filename) => {
  const fileStream = fs.createReadStream(filename);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });
  let rowCount = 0;
  for await (const _ of rl) {
    rowCount++;
  }
  return rowCount - 1; // header
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const readCSVAndUploadRows = async (filename, uploadUrl) => {
  const totalRows = await countRowsInCSV(filename);
  progressBar.start(totalRows, 0);

  const fileContent = fs.readFileSync(filename, "utf8");

  const parsedCSV = Papa.parse(fileContent, {
    header: true,
    dynamicTyping: false,
  });

  for (const row of parsedCSV.data) {
    await uploadRow(uploadUrl, row);
    progressBar.increment();
    // await delay(60);s
  }

  progressBar.stop();
  process.exit(0);
};

const [, , filename] = process.argv;
if (!filename) {
  console.error("");
  process.exit(1);
}

const uploadUrl = `https://budibase.app/api/public/v1/tables/${process.env.TABLE_ID}/rows`;

readCSVAndUploadRows(filename, uploadUrl)
  .then(() => {
    console.log("upload complete");
  })
  .catch((err) => {
    console.error(err);
  });
