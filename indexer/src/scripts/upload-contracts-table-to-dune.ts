import { In } from "typeorm";
import { AppDataSource } from "../db/data-source.js";
import {
  Artifact,
  ArtifactNamespace,
  ArtifactType,
} from "../db/orm-entities.js";
import { ProjectRepository } from "../db/project.js";
import fetch from "node-fetch";
import { DUNE_API_KEY } from "../config.js";
import { writeFile } from "fs/promises";
import { UniqueArray } from "../utils/array.js";

export async function main() {
  await AppDataSource.initialize();

  const projects = await ProjectRepository.find({
    relations: {
      artifacts: true,
    },
    where: {
      artifacts: {
        type: In([ArtifactType.CONTRACT_ADDRESS]),
        namespace: ArtifactNamespace.OPTIMISM,
      },
    },
  });
  const allArtifacts = projects.flatMap((p) => p.artifacts);

  const uniqueArtifacts = new UniqueArray((a: Artifact) => a.id);
  allArtifacts.forEach((a) => uniqueArtifacts.push(a));

  const rows = ["id,address"];
  rows.push(
    ...uniqueArtifacts.items().map((a) => {
      return `${a.id}, ${a.name}`;
    }),
  );
  const artifactsCsv = rows.join("\n");

  await writeFile("contracts-v1.csv", artifactsCsv, { encoding: "utf-8" });

  const uploader = new DuneCSVUploader(DUNE_API_KEY);
  const response = await uploader.upload(
    "oso_optimism_contracts_v00001",
    "OSO monitored optimism contracts",
    rows,
  );
  if (response.status !== 200) {
    console.log("failed to upload to the contracts");
  }
}

class DuneCSVUploader {
  private apiKey: string;

  constructor(apiKey: string) {
    this.apiKey = apiKey;
  }

  async upload(tableName: string, description: string, rows: string[]) {
    return await fetch("https://api.dune.com/api/v1/table/upload/csv", {
      method: "POST",
      body: JSON.stringify({
        table_name: tableName,
        description: description,
        data: rows.join("\n"),
      }),
      headers: {
        "Content-Type": "application/json",
        "X-Dune-Api-Key": this.apiKey,
      },
    });
  }
}

main().catch((err) => {
  console.log(err);
});
