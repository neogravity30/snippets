const fs = require('fs');
const readline = require('readline');

async function processFile(filePath, outputFilePath) {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity // To recognize all instances of CR LF as a single line break
    });

    let currentRecordLines = [];
    const outputStream = fs.createWriteStream(outputFilePath);

    // Write CSV header (if you have one, otherwise remove this line)
    // Based on your example: dcn,first_name,last_name,queue_name,event,datetime,user_notes,system_notes,description
    outputStream.write("dcn,first_name,last_name,queue_name,event,datetime,user_notes,system_notes,description\n");

    const dcnRegex = /^\d{6,}\|/; // Matches 6 or more digits followed by a pipe at the start of the line

    for await (const line of rl) {
        // Trim the line to remove any leading/trailing whitespace
        const trimmedLine = line.trim();

        // Skip empty lines
        if (trimmedLine === '') {
            continue;
        }

        // Check if the line starts a new record
        if (dcnRegex.test(trimmedLine)) {
            // If we have a buffered record, process it before starting a new one
            if (currentRecordLines.length > 0) {
                const csvLine = formatRecordToCsv(currentRecordLines);
                outputStream.write(csvLine + '\n');
            }
            // Start a new record
            currentRecordLines = [trimmedLine];
        } else {
            // Continuation of the current record
            currentRecordLines.push(trimmedLine);
        }
    }

    // Process any remaining record at the end of the file
    if (currentRecordLines.length > 0) {
        const csvLine = formatRecordToCsv(currentRecordLines);
        outputStream.write(csvLine + '\n');
    }

    console.log(`File processing complete. CSV output written to ${outputFilePath}`);
    outputStream.end();
}

function formatRecordToCsv(lines) {
    // 1. Join all parts of the record
    let joinedRecord = lines.join('').trim();

    // 2. Normalize multiple spaces and ensure consistent delimiters
    // Replace multiple pipes with single pipe, and remove leading/trailing pipes
    joinedRecord = joinedRecord.replace(/\|+/g, '|').replace(/^\||\|$/g, '');

    // 3. Split by pipe and handle potential commas in fields by quoting
    const fields = joinedRecord.split('|').map(field => {
        // Trim each field to remove extra spaces
        field = field.trim();
        // If a field contains a comma, enclose it in double quotes
        // Also, if it contains double quotes, escape them by doubling them
        if (field.includes(',') || field.includes('"')) {
            return `"${field.replace(/"/g, '""')}"`;
        }
        return field;
    });

    // 4. Ensure 7 fields - if not, add empty strings for missing ones (adjust based on your exact schema)
    // This part is crucial if your "7 fields" rule is strict and some are truly missing
    // Based on your example, it seems more like the data just flows into the next line if the DCN isn't there.
    // The previous steps should handle combining the data correctly.
    // However, if you need to strictly enforce 7 fields and fill in blanks, you might do something like:
    // while (fields.length < 9) { // 9 fields as per header above
    //     fields.push('');
    // }
    // fields.splice(9); // Truncate if there are more than 9 fields (unlikely if parsing logic is correct)

    return fields.join(',');
}


processFile(inputFileName, outputFileName).catch(err => {
    console.error('An error occurred:', err);
});
