// Sanitized derivative; server-folder flow supplies decoded content despite historical email wording.
/**
 * Office Script for Power Automate.
 *
 * Converts a pipe-delimited NOSHOWDATA email attachment into a formatted
 * worksheet. The Power Automate flow must copy a blank .xlsx template first,
 * then run this script against that workbook.
 *
 * Power Automate inputs:
 *   fileContent    - decoded text content of the email attachment
 *   sourceFileName - original attachment filename, for example
 *                    DEMO-NOSHOWDATA-20260730100118.txt
 */
function main(
  workbook: ExcelScript.Workbook,
  fileContent: string,
  sourceFileName: string
): {
  status: string;
  sourceFileName: string;
  reportDate: string;
  rowsWritten: number;
  paddedRows: number;
  ignoredRows: number;
} {
  const requiredColumns: string[] = [
    "CLINICID",
    "DOCTORID",
    "DOCTORNAME",
    "APPTDTTM",
    "APPTID",
    "APPTNATUREID",
    "VISITURN",
    "BOOKINGDT",
    "SPECIALTYID",
    "SCHLDDT",
    "ADMINSVCID",
    "ADVANCE",
    "ADVADJUST",
    "ADVANCEDT",
    "ADVADJUSTPYBDT"
  ];

  const outputColumns: string[] = ["report_date", ...requiredColumns];

  if (!fileContent || fileContent.trim().length === 0) {
    throw new Error(
      "fileContent is empty. Pass the decoded text of the email attachment."
    );
  }

  if (!sourceFileName || sourceFileName.trim().length === 0) {
    throw new Error(
      "sourceFileName is empty. Pass the original email attachment name."
    );
  }

  // Extract YYYYMMDD and ignore the following HHMMSS portion.
  const filenameMatch: RegExpMatchArray | null = sourceFileName.match(
    /DEMO-NOSHOWDATA-(\d{8})(?:\d{6})?/i
  );

  if (!filenameMatch) {
    throw new Error(
      "Unexpected filename. Expected " +
      "DEMO-NOSHOWDATA-YYYYMMDDHHMMSS; found " +
      sourceFileName
    );
  }

  const reportDateText: string = filenameMatch[1];
  const reportYear: number = Number(reportDateText.substring(0, 4));
  const reportMonth: number = Number(reportDateText.substring(4, 6));
  const reportDay: number = Number(reportDateText.substring(6, 8));

  const validationDate: Date = new Date(
    Date.UTC(reportYear, reportMonth - 1, reportDay)
  );

  if (
    validationDate.getUTCFullYear() !== reportYear ||
    validationDate.getUTCMonth() + 1 !== reportMonth ||
    validationDate.getUTCDate() !== reportDay
  ) {
    throw new Error(
      "Invalid report date " + reportDateText + " in " + sourceFileName
    );
  }

  const formattedReportDate: string =
    padTwoDigits(reportMonth) +
    "/" +
    padTwoDigits(reportDay) +
    "/" +
    reportYear.toString();

  // Excel date serial for a genuine date value, not a text value.
  const reportDateSerial: number =
    Date.UTC(reportYear, reportMonth - 1, reportDay) / 86400000 + 25569;

  // Remove a possible byte-order mark, split lines, and ignore blank lines.
  const cleanedContent: string = fileContent.replace(/^\uFEFF/, "");
  const nonEmptyLines: string[] = cleanedContent
    .split(/\r?\n/)
    .filter((line: string) => line.trim().length > 0);

  if (nonEmptyLines.length < 4) {
    throw new Error(
      "Expected an H record, a C header, at least one D record, " +
      "and a final trailer record."
    );
  }

  const removedFirstLine: string = nonEmptyLines[0];
  const removedLastLine: string = nonEmptyLines[nonEmptyLines.length - 1];
  const contentLines: string[] = nonEmptyLines.slice(1, -1);

  console.log("Removed first line: " + removedFirstLine.substring(0, 200));
  console.log("Removed last line: " + removedLastLine.substring(0, 200));

  if (contentLines.length < 2) {
    throw new Error("No data remains after removing the first and last lines.");
  }

  // The first retained record must be C|CLINICID|DOCTORID|...
  const rawHeader: string[] = contentLines[0]
    .split("|")
    .map((value: string) => value.trim());

  if (rawHeader.length === 0 || rawHeader[0].toUpperCase() !== "C") {
    throw new Error(
      "The second non-empty source line must be the C column-header record."
    );
  }

  const sourceColumns: string[] = rawHeader.slice(1);
  const normalizedSourceColumns: string[] = sourceColumns.map(
    (column: string) => column.toUpperCase()
  );

  const requiredColumnIndexes: number[] = requiredColumns.map(
    (column: string) => normalizedSourceColumns.indexOf(column.toUpperCase())
  );

  const missingColumns: string[] = [];
  requiredColumnIndexes.forEach((columnIndex: number, index: number) => {
    if (columnIndex === -1) {
      missingColumns.push(requiredColumns[index]);
    }
  });

  if (missingColumns.length > 0) {
    throw new Error(
      "Required columns missing from the C header: " +
      missingColumns.join(", ")
    );
  }

  const outputRows: (string | number | boolean)[][] = [];
  let paddedRows: number = 0;
  let ignoredRows: number = 0;

  // contentLines[0] is the C header; remaining lines should be D records.
  for (let lineIndex: number = 1; lineIndex < contentLines.length; lineIndex++) {
    const rawValues: string[] = contentLines[lineIndex]
      .split("|")
      .map((value: string) => value.trim());

    if (rawValues.length === 0) {
      continue;
    }

    if (rawValues[0].toUpperCase() !== "D") {
      ignoredRows++;
      continue;
    }

    // Remove the leading D record-type field.
    const businessValues: string[] = rawValues.slice(1);

    // Some records omit empty optional fields at the right edge. Pad only on
    // the right, so existing values remain aligned to the C header.
    if (businessValues.length < sourceColumns.length) {
      paddedRows++;
      while (businessValues.length < sourceColumns.length) {
        businessValues.push("");
      }
    }

    const outputRow: (string | number | boolean)[] = [reportDateSerial];

    requiredColumnIndexes.forEach((sourceIndex: number) => {
      outputRow.push(
        sourceIndex < businessValues.length ? businessValues[sourceIndex] : ""
      );
    });

    outputRows.push(outputRow);
  }

  if (outputRows.length === 0) {
    throw new Error("No valid D records were found in the source file.");
  }

  let worksheet: ExcelScript.Worksheet;
  const existingWorksheet: ExcelScript.Worksheet | undefined =
    workbook.getWorksheet("NoshowData");

  if (existingWorksheet) {
    worksheet = existingWorksheet;
  } else {
    worksheet = workbook.addWorksheet("NoshowData");
  }

  // Remove an old table before rebuilding the sheet during a rerun.
  worksheet.getTables().forEach((table: ExcelScript.Table) => table.delete());

  const existingRange: ExcelScript.Range | undefined = worksheet.getUsedRange();
  if (existingRange) {
    existingRange.clear(ExcelScript.ClearApplyTo.all);
  }

  const headerRange: ExcelScript.Range = worksheet.getRangeByIndexes(
    0,
    0,
    1,
    outputColumns.length
  );
  headerRange.setValues([outputColumns]);

  const dataRange: ExcelScript.Range = worksheet.getRangeByIndexes(
    1,
    0,
    outputRows.length,
    outputColumns.length
  );
  dataRange.setValues(outputRows);

  // Format report_date as MM/DD/YYYY.
  const reportDateRange: ExcelScript.Range = worksheet.getRangeByIndexes(
    1,
    0,
    outputRows.length,
    1
  );
  reportDateRange.setNumberFormatLocal(
    Array(outputRows.length).fill(["mm/dd/yyyy"])
  );

  const headerFormat: ExcelScript.RangeFormat = headerRange.getFormat();
  headerFormat.getFill().setColor("#1F4E78");
  headerFormat.getFont().setColor("#FFFFFF");
  headerFormat.getFont().setBold(true);
  headerFormat.setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
  headerFormat.setVerticalAlignment(ExcelScript.VerticalAlignment.center);

  const completeRange: ExcelScript.Range = worksheet.getRangeByIndexes(
    0,
    0,
    outputRows.length + 1,
    outputColumns.length
  );

  const outputTable: ExcelScript.Table = worksheet.addTable(completeRange, true);
  outputTable.setName("NoshowDataTable");
  outputTable.setPredefinedTableStyle("TableStyleMedium2");

  completeRange.getFormat().autofitColumns();

  // Limit excessively wide text columns after autofit.
  const maximumColumnWidth: number = 220;
  for (
    let columnIndex: number = 0;
    columnIndex < outputColumns.length;
    columnIndex++
  ) {
    const columnRange: ExcelScript.Range = worksheet.getRangeByIndexes(
      0,
      columnIndex,
      outputRows.length + 1,
      1
    );

    if (columnRange.getFormat().getColumnWidth() > maximumColumnWidth) {
      columnRange.getFormat().setColumnWidth(maximumColumnWidth);
    }
  }

  completeRange.getFormat().autofitRows();
  worksheet.getFreezePanes().freezeRows(1);
  worksheet.activate();

  console.log("Rows written: " + outputRows.length.toString());
  console.log("Rows padded: " + paddedRows.toString());
  console.log("Rows ignored: " + ignoredRows.toString());

  return {
    status: "SUCCESS",
    sourceFileName: sourceFileName,
    reportDate: formattedReportDate,
    rowsWritten: outputRows.length,
    paddedRows: paddedRows,
    ignoredRows: ignoredRows
  };
}

function padTwoDigits(value: number): string {
  return value.toString().padStart(2, "0");
}
