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
  .option("-l, --limit <limit>", "Documents limit per request")
  .option("-f, --file <file>", "Filename you want to restore")
  .action(
    async ({ action, endpoint, apiKey, project, collection, limit, file }) => {
      limit = limit ? limit : 10;

      const sdk = new appwrite.Client();
      const db = new appwrite.Database(sdk);

      sdk.setEndpoint(endpoint).setProject(project).setKey(apiKey);

      let documentsAmount = 0;
      switch (action.toLowerCase()) {
        //! documents-wipe
        case "documents-wipe":
          console.log("Wiping documents, this can take a while ...");

          let documentsFound = 0;
          do {
            const appwriteResponse = await db.listDocuments(
              collection,
              [],
              limit
            );

            for (const document of appwriteResponse.documents) {
              await db.deleteDocument(collection, document.$id);
              documentsAmount++;
            }

            documentsFound = appwriteResponse.documents.length;
          } while (documentsFound > 0);

          console.log(`All ${documentsAmount} documents wiped.`);
          break;

        //! documents-backup
        case "documents-backup":
          console.log("Backing up documents, this can take a while ...");

          const fileName = `backup_${Date.now()}.csv`;

          const writer = csvWriter();
          writer.pipe(fs.createWriteStream(fileName));
          let cursor = undefined;
          try {
            do {
              const appwriteResponse = await db.listDocuments(
                collection,
                [],
                limit,
                undefined,
                cursor,
                "after"
              );

              for (const document of appwriteResponse.documents) {
                const obj = {
                  id: document["$id"],
                  read: JSON.stringify(document["$read"]),
                  write: JSON.stringify(document["$write"]),
                  data: null,
                };

                const documentCopy = { ...document };
                delete documentCopy["$internalId"];
                delete documentCopy["$collection"];
                delete documentCopy["$id"];
                delete documentCopy["$read"];
                delete documentCopy["$write"];
                obj.data = JSON.stringify(documentCopy);

                writer.write(obj);
              }
              documentsAmount += appwriteResponse.documents.length;

              if (appwriteResponse.documents.length > 0) {
                cursor =
                  appwriteResponse.documents[
                    appwriteResponse.documents.length - 1
                  ].$id;
              } else {
                cursor = undefined;
              }
            } while (cursor !== undefined);

            writer.end();

            console.log(
              `All ${documentsAmount} documents stored in ${fileName}.`
            );
          } catch (e) {
            fs.unlinkSync(fileName);
            console.error(e);
          }
          break;

        //! documents-restore
        case "documents-restore":
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
            const read = JSON.parse(row["read"]);
            const write = JSON.parse(row["write"]);

            await db.createDocument(
              collection,
              id,
              JSON.parse(row["data"]),
              read,
              write
            );

            totalRows++;
          }

          console.log(
            `All ${totalRows} documents stored into collection ${collection}.`
          );
          break;
        default:
          console.error("Unsupported action.");
          break;
      }
    }
  );

program.parse(process.argv);
