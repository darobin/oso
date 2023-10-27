import { AppDataSource } from "./data-source.js";
import { ExternalDataExportPointer } from "./orm-entities.js";

export const ExternalDataExportPointerRepository = AppDataSource.getRepository(
  ExternalDataExportPointer,
).extend({
  async getLatestForName(name: string): Promise<ExternalDataExportPointer[]> {
    return this.find({
      where: {
        name: name,
      },
      order: {
        updatedAt: { direction: "DESC" },
      },
      take: 1,
    });
  },
});
