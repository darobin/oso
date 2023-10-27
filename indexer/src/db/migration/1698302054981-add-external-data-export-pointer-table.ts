import { MigrationInterface, QueryRunner } from "typeorm";

export class AddExternalDataExportPointerTable1698302054981
  implements MigrationInterface
{
  name = "AddExternalDataExportPointerTable1698302054981";

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "external_data_export_pointer" ("id" SERIAL NOT NULL, "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "deletedAt" TIMESTAMP WITH TIME ZONE, "name" text NOT NULL, "digest" character varying(64) NOT NULL, "path" text NOT NULL, "details" jsonb NOT NULL DEFAULT '{}', CONSTRAINT "PK_4bd87de9f88149d68d4d039d6cb" PRIMARY KEY ("id"))`,
    );
    await queryRunner.query(
      `CREATE UNIQUE INDEX "IDX_866035322bda76f9dc5740e22e" ON "external_data_export_pointer" ("name", "digest") `,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `DROP INDEX "public"."IDX_866035322bda76f9dc5740e22e"`,
    );
    await queryRunner.query(`DROP TABLE "external_data_export_pointer"`);
  }
}
