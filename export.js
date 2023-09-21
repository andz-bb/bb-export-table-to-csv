import dotenv from "dotenv";
import fetch from "node-fetch";
import fs from "fs";
import { SingleBar } from "cli-progress";

dotenv.config();

const progressBar = new SingleBar({
  format: "Fetching rows... | {bar} | {percentage}% | {value} Rows",
  barCompleteChar: "\u2588",
  barIncompleteChar: "\u2591",
  hideCursor: true,
});

const fetchRows = async (url, bookmark = null, accumulatedRows = []) => {
  const fetchOptions = {
    method: "POST",
    headers: {
      accept: "application/json",
      "x-budibase-app-id": process.env.APP_ID,
      "content-type": "application/json",
      "x-budibase-api-key": process.env.BUDIBASE_API_KEY,
    },
    body: JSON.stringify({ paginate: true, limit: 5000, bookmark }),
  };

  const response = await fetch(url, fetchOptions);
  const data = await response.json();
  accumulatedRows.push(...data.data);

  progressBar.start(data.totalCount, 0);
  progressBar.update(accumulatedRows.length);

  return data.hasNextPage
    ? fetchRows(url, data.bookmark, accumulatedRows)
    : accumulatedRows;
};

const convertToCSV = (rows) => {
  const columns = new Set();
  rows.forEach((row) =>
    Object.keys(row).forEach((column) => columns.add(`"${column}"`))
  );

  const csvRows = [Array.from(columns).join(",")];

  rows.forEach((row) => {
    const values = Array.from(columns).map((column) => {
      const columnName = column.replace(/"/g, "");
      const value = row[columnName];
      return (typeof value === "object" && value !== null) ||
        value === undefined ||
        value === null
        ? ""
        : `"${value}"`;
    });
    csvRows.push(values.join(","));
  });

  return csvRows.join("\n");
};

const saveToCSVFile = (content, filename) => {
  fs.writeFileSync(filename, content);
  console.log(`CSV file saved as ${filename}`);
  process.exit(0);
};

const timestamp = new Date().toISOString().replace(/[:.-]/g, "");
const outputFilename = `output_${timestamp}.csv`;

fetchRows(
  `https://budibase.app/api/public/v1/tables/${process.env.TABLE_ID}/rows/search`
)
  .then((rows) => {
    console.log(`Rows fetched: ${rows.length}`);
    saveToCSVFile(convertToCSV(rows), outputFilename);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
