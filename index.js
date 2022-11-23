const commander = require("commander");
const appwrite = require("node-appwrite");
const csvWriter = require("csv-write-stream");
const fs = require("fs");
const csv = require("csv-parser");
const readline = require("readline");

const program = new commander.Command();

program
 .version("0.0.1")
 .requiredOption("-e, --endpoint <endpoint>", "Appwrite API Endpoint")
 .requiredOption(
  "-a, --action <action>",
  "Action to run. Can be 'documents-backup' or 'documents-restore' or 'documents-wipe'"
 )
 .requiredOption("-k, --api-key <apiKey>", "Appwrite API Key")
 .requiredOption("-p, --project <projectId>", "Project ID")
 .requiredOption("-c, --collection <collectionId>", "Collection ID")
 .requiredOption("-d, --database <databaseId>", "Database ID")
 .option("-l, --limit <limit>", "Documents limit per request")
 .option("-f, --file <file>", "Filename you want to restore")
 .action(
  async ({ action, endpoint, apiKey, project, collection, database, limit, file }) => {
   limit = limit ? limit : 100;

   const sdk = new appwrite.Client();
   const db = new appwrite.Databases(sdk);

   sdk.setEndpoint(endpoint).setProject(project).setKey(apiKey);

   if (action === "documents-wipe") {
    console.log("Wiping documents, this can take a while ...");

    let documentsFound = 0;
    let documentsAmount = 0;
    do {
     const appwriteResponse = await db.listDocuments(database, collection, [ appwrite.Query.limit(limit) ]);

     for (const document of appwriteResponse.documents) {
      await db.deleteDocument(database, collection, document.$id);
      documentsAmount++;
     }

     documentsFound = appwriteResponse.documents.length;
    } while (documentsFound > 0);

    console.log(`All ${documentsAmount} documents wiped.`);
   } else if (action === "documents-backup") {
    console.log("Backing up documents, this can take a while ...");

    const fileName = `backup_${database}_${collection}_${Date.now()}.csv`;

    const writer = csvWriter();
    writer.pipe(fs.createWriteStream(fileName));

    let cursor = undefined;
    let documentsAmount = 0;
    do {
      const queries = [
        appwrite.Query.limit(limit)
      ];

      if(cursor) {
        queries.push(appwrite.Query.cursorAfter(cursor));
      }

     const appwriteResponse = await db.listDocuments(database, collection, queries);

     for (const document of appwriteResponse.documents) {
      const obj = {
       id: document["$id"],
       permissions: JSON.stringify(document["$permissions"]),
       data: null,
      };

      const documentCopy = { ...document };
      delete documentCopy["$collectionId"];
      delete documentCopy["$createdAt"];
      delete documentCopy["$updatedAt"];
      delete documentCopy["$id"];
      delete documentCopy["$permissions"];
      obj.data = JSON.stringify(documentCopy);

      writer.write(obj);
     }
     documentsAmount += appwriteResponse.documents.length;

     if (appwriteResponse.documents.length > 0) {
      cursor =
       appwriteResponse.documents[appwriteResponse.documents.length - 1].$id;
     } else {
      cursor = undefined;
     }
    } while (cursor !== undefined);

    writer.end();

    console.log(`All ${documentsAmount} documents stored in ${fileName}.`);
   } else if (action === "documents-restore") {
    if (!file) {
     console.error("-f is not optional when restoring docuements.");
     return;
    }

    console.log("Restoring documents, this can take a while ...");

    const stream = fs.createReadStream(file);

    const rl = readline.createInterface({
     input: stream,
     crlfDelay: Infinity,
    });

    let totalRows = 0;
    let firstLine;
    for await (const line of rl) {
     if (!firstLine) {
      firstLine = line.split(",");
      continue;
     }

     let row;

     const csvObject = csv(firstLine);
     csvObject.on("data", (_row) => {
      if (!row) {
       row = _row;
      }
     });
     csvObject.write(line + "\n\r");
     csvObject.end();

     const id = row["id"];
     const permissions = JSON.parse(row["permissions"]);

     await db.createDocument(
      database,
      collection,
      id,
      JSON.parse(row["data"]),
      permissions
     );

     totalRows++;
    }

    console.log(
     `All ${totalRows} documents stored into collection ${collection}.`
    );
   } else {
    console.error("Unsupported action.");
   }
  }
 );

program.parse(process.argv);
