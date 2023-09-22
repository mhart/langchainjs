import { Document } from "../../document.js";
import { BaseDocumentLoader } from "../base.js";

/**
 * Represents an element returned by the Unstructured API. It has
 * properties for the element type, text content, and metadata.
 */
type Element = {
  type: string;
  text: string;
  // this is purposefully loosely typed
  metadata: {
    [key: string]: unknown;
  };
};

/**
 * Represents the available strategies for the UnstructuredLoader. It can
 * be one of "hi_res", "fast", "ocr_only", or "auto".
 */
export type UnstructuredLoaderStrategy =
  | "hi_res"
  | "fast"
  | "ocr_only"
  | "auto";

/**
 * Represents a string value with autocomplete suggestions. It is used for
 * the `strategy` property in the UnstructuredLoaderOptions.
 */
type StringWithAutocomplete<T> = T | (string & Record<never, never>);

export type UnstructuredLoaderOptions = {
  apiKey?: string;
  apiUrl?: string;
  strategy?: StringWithAutocomplete<UnstructuredLoaderStrategy>;
  encoding?: string;
  ocrLanguages?: Array<string>;
  coordinates?: boolean;
  pdfInferTableStructure?: boolean;
  xmlKeepTags?: boolean;
};

/**
 * A document loader that uses the Unstructured API to load unstructured
 * documents. It supports both the new syntax with options object and the
 * legacy syntax for backward compatibility. The load() method sends a
 * partitioning request to the Unstructured API and retrieves the
 * partitioned elements. It creates a Document instance for each element
 * and returns an array of Document instances.
 */
export class UnstructuredWebLoader extends BaseDocumentLoader {
  public filePath: string;

  public buffer: Uint8Array;

  private apiUrl = "https://api.unstructured.io/general/v0/general";

  private apiKey?: string;

  private strategy: StringWithAutocomplete<UnstructuredLoaderStrategy> =
    "hi_res";

  private encoding?: string;

  private ocrLanguages: Array<string> = [];

  private coordinates?: boolean;

  private pdfInferTableStructure?: boolean;

  private xmlKeepTags?: boolean;

  constructor(
    filePath: string,
    buffer: Uint8Array,
    options: UnstructuredLoaderOptions = {}
  ) {
    super();

    this.filePath = filePath;
    this.buffer = buffer;
    this.apiKey = options.apiKey;
    this.apiUrl = options.apiUrl ?? this.apiUrl;
    this.strategy = options.strategy ?? this.strategy;
    this.encoding = options.encoding;
    this.ocrLanguages = options.ocrLanguages ?? this.ocrLanguages;
    this.coordinates = options.coordinates;
    this.pdfInferTableStructure = options.pdfInferTableStructure;
    this.xmlKeepTags = options.xmlKeepTags;
  }

  async _partition() {
    const fileName = this.filePath.split("/").pop();

    const formData = new FormData();
    formData.append("files", new Blob([this.buffer]), fileName);
    formData.append("strategy", this.strategy);
    this.ocrLanguages.forEach((language) => {
      formData.append("ocr_languages", language);
    });
    if (this.encoding) {
      formData.append("encoding", this.encoding);
    }
    if (this.coordinates === true) {
      formData.append("coordinates", "true");
    }
    if (this.pdfInferTableStructure === true) {
      formData.append("pdf_infer_table_structure", "true");
    }
    if (this.xmlKeepTags === true) {
      formData.append("xml_keep_tags", "true");
    }

    const headers = {
      "UNSTRUCTURED-API-KEY": this.apiKey ?? "",
    };

    const response = await fetch(this.apiUrl, {
      method: "POST",
      body: formData,
      headers,
    });

    if (!response.ok) {
      throw new Error(
        `Failed to partition file ${this.filePath} with error ${
          response.status
        } and message ${await response.text()}`
      );
    }

    const elements = await response.json();
    if (!Array.isArray(elements)) {
      throw new Error(
        `Expected partitioning request to return an array, but got ${elements}`
      );
    }
    return elements.filter((el) => typeof el.text === "string") as Element[];
  }

  async load(): Promise<Document[]> {
    const elements = await this._partition();

    const documents: Document[] = [];
    for (const element of elements) {
      const { metadata, text } = element;
      if (typeof text === "string") {
        documents.push(
          new Document({
            pageContent: text,
            metadata: {
              ...metadata,
              category: element.type,
            },
          })
        );
      }
    }

    return documents;
  }
}
