using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Azure.Storage;

namespace ps_salesapp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Getting started with the Sales Data Management application..");
            Console.WriteLine();
            // Azure Blob Storage Properties
            Console.WriteLine("Settting up updated Azure Connection Storage details");
            string storageaccount = "##Strorage Account Name##";
            string connectionString = "##Connection String##";
            string SharedAccessURI = "##Shared Access Signature URI##";
            string container = "ps-sales";
            string filename = "Sales-2019.csv";

            Console.WriteLine();
            Console.WriteLine ("Listing all the Sales data in the container " + container + " in Storage Account " + storageaccount);
            Console.WriteLine();
            
            SalesList(connectionString, container);
            Console.WriteLine();
            Console.WriteLine("Provide the name of the file to be uploaded to the blob storage");
            Console.WriteLine();
            
            filename = Console.ReadLine();
            Console.WriteLine ("Uploading the file " + filename + " in the container " + container + " in Storage Account " + storageaccount);
            Console.WriteLine();
            
            SalesUpload(connectionString, container, filename);
            Console.WriteLine();
            Console.WriteLine ("Listing all the Sales data in the container " + container + " in Storage Account " + storageaccount + "after upload completion");
            Console.WriteLine();
            
            SalesList(connectionString, container);
            Console.WriteLine();
            Console.WriteLine ("Reading the file " + filename + " from the container " + container + " in Storage Account " + storageaccount);
            Console.WriteLine();
            SalesView(storageaccount,SharedAccessURI,container,filename).GetAwaiter().GetResult();
            Console.WriteLine();
            Console.WriteLine ("Completed Execution");
        }


        //Method to upload a file to a Blob.
        private static void SalesUpload(string ConnectionString, string Container, String filename)
        {
            BlobContainerClient blobcontainer = new BlobContainerClient(ConnectionString, Container);
            
            if(blobcontainer.GetBlobClient(filename).Exists())
            {
            blobcontainer.DeleteBlob(filename);
            }
            
            if(File.Exists(filename))
            {
                BlobClient blob = blobcontainer.GetBlobClient(filename);
                using (FileStream file = File.OpenRead(filename))
                {
                    blob.Upload(file);
                    Console.WriteLine("The File " + filename + " is being uploaded");
                }
                Console.WriteLine("The File " + filename + "is uploaded successfully");
            }
            else
            {
                Console.WriteLine("The file doesn't exist, Please provide a file name that exists in the folder data");
            }
        }

        //Method to list all Blob Objects in the container using Connection string.
        private static void SalesList(string ConnectionString, string Container)
        {
            string connectionString = ConnectionString;
            BlobContainerClient container = new BlobContainerClient(connectionString, Container);
            
            container.CreateAsync();
            foreach (BlobItem blob in container.GetBlobs())
            {
                Console.WriteLine(blob.Name);
            }
        }

        //Aysnchronous Task to read the data from a blob using Shared Access Signature
        public static async Task SalesView(string StorageAccount, string URI, string Container, String filename)
        {
            
            UriBuilder sasURI = new UriBuilder(URI);
            BlobServiceClient service = new BlobServiceClient(sasURI.Uri);
            BlobClient blbclient = service.GetBlobContainerClient(Container).GetBlobClient(filename);

            if(await blbclient.ExistsAsync())
            {
                var blobres = await blbclient.DownloadAsync();
                using (var streamReader = new StreamReader(blobres.Value.Content))
                {
                    while (!streamReader.EndOfStream)
                    {
                        var line = await streamReader.ReadLineAsync();
                        Console.WriteLine(line);
                    }
                }
            }
        }
    }
}
