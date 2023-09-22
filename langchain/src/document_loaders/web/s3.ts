import { S3Client, GetObjectCommand, S3ClientConfig } from "@aws-sdk/client-s3";
import { BaseDocumentLoader } from "../base.js";
import { UnstructuredWebLoader as UnstructuredLoaderDefault } from "./unstructured.js";

/**
 * Represents the configuration options for the S3 client. It extends the
 * S3ClientConfig interface from the "@aws-sdk/client-s3" package and
 * includes additional deprecated properties for access key ID and secret
 * access key.
 */
export type S3Config = S3ClientConfig & {
  /** @deprecated Use the credentials object instead */
  accessKeyId?: string;
  /** @deprecated Use the credentials object instead */
  secretAccessKey?: string;
};

/**
 * Represents the parameters for the S3Loader class. It includes
 * properties such as the S3 bucket, key, unstructured API URL,
 * unstructured API key, S3 configuration, file system module, and
 * UnstructuredLoader module.
 */
export interface S3LoaderParams {
  bucket: string;
  key: string;
  unstructuredAPIURL: string;
  unstructuredAPIKey: string;
  s3Config?: S3Config & {
    /** @deprecated Use the credentials object instead */
    accessKeyId?: string;
    /** @deprecated Use the credentials object instead */
    secretAccessKey?: string;
  };
  UnstructuredLoader?: typeof UnstructuredLoaderDefault;
}

/**
 * A class that extends the BaseDocumentLoader class. It represents a
 * document loader for loading files from an S3 bucket.
 */
export class S3Loader extends BaseDocumentLoader {
  private bucket: string;

  private key: string;

  private unstructuredAPIURL: string;

  private unstructuredAPIKey: string;

  private s3Config: S3Config & {
    /** @deprecated Use the credentials object instead */
    accessKeyId?: string;
    /** @deprecated Use the credentials object instead */
    secretAccessKey?: string;
  };

  private _UnstructuredLoader: typeof UnstructuredLoaderDefault;

  constructor({
    bucket,
    key,
    unstructuredAPIURL,
    unstructuredAPIKey,
    s3Config = {},
    UnstructuredLoader = UnstructuredLoaderDefault,
  }: S3LoaderParams) {
    super();
    this.bucket = bucket;
    this.key = key;
    this.unstructuredAPIURL = unstructuredAPIURL;
    this.unstructuredAPIKey = unstructuredAPIKey;
    this.s3Config = s3Config;
    this._UnstructuredLoader = UnstructuredLoader;
  }

  /**
   * Loads the file from the S3 bucket into memory,
   * and then uses the UnstructuredLoader to load the file as a document.
   * @returns An array of Document objects representing the loaded documents.
   */
  public async load() {
    const filePath = this.key;
    let objectData: Uint8Array;

    try {
      const s3Client = new S3Client(this.s3Config);

      const getObjectCommand = new GetObjectCommand({
        Bucket: this.bucket,
        Key: this.key,
      });

      const response = await s3Client.send(getObjectCommand);

      objectData = await response.Body.transformToByteArray();

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (e: any) {
      throw new Error(
        `Failed to download file ${this.key} from S3 bucket ${this.bucket}: ${e.message}`
      );
    }

    try {
      const options = {
        apiUrl: this.unstructuredAPIURL,
        apiKey: this.unstructuredAPIKey,
      };

      const unstructuredLoader = new this._UnstructuredLoader(
        filePath,
        objectData,
        options
      );

      const docs = await unstructuredLoader.load();

      return docs;
    } catch {
      throw new Error(
        `Failed to load file ${filePath} using unstructured loader.`
      );
    }
  }
}
