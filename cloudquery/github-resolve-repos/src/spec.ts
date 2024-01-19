import { default as Ajv } from "ajv";
import camelcaseKeys from "camelcase-keys";

const spec = {
  type: "object",
  properties: {
    concurrency: { type: "integer" },
    token: { type: "string" },
    projectsInputPath: { type: "string" },
  },
  required: ["token"],
};

const ajv = new Ajv.default();
const validate = ajv.compile(spec);

export type Spec = {
  concurrency: number;
  token: string;
  projectsInputPath: string;
};

export const parseSpec = (spec: string): Spec => {
  const parsed = JSON.parse(spec) as Partial<Spec>;
  const valid = validate(parsed);
  if (!valid) {
    throw new Error(`Invalid spec: ${JSON.stringify(validate.errors)}`);
  }
  const {
    concurrency = 10_000,
    token = "",
    projectsInputPath = "",
  } = camelcaseKeys(parsed);
  return { concurrency, token, projectsInputPath };
};
