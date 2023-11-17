// Testing tools for the recorder
import { randomInt, randomUUID } from "crypto";
import {
  IEventRecorderClient,
  IncompleteEvent,
  RecordHandle,
} from "./types.js";
import { ArtifactNamespace, ArtifactType } from "../index.js";
import { DateTime } from "luxon";
import _ from "lodash";
import { AsyncResults } from "../utils/async-results.js";

export type RandomCommitEventOptions = {
  fromProbability: number;
  repoNameGenerator: () => string;
  usernameGenerator: () => string;
};

export class FakeRecorderClient implements IEventRecorderClient {
  async record(event: IncompleteEvent): Promise<RecordHandle> {
    return {
      id: event.sourceId,
      wait: async () => {
        return event.sourceId;
      },
    };
  }

  async wait(
    _handles: RecordHandle[],
    _timeoutMs?: number | undefined,
  ): Promise<AsyncResults<string>> {
    return {
      errors: [],
      success: [],
    };
  }
}

export function randomCommitEventsGenerator(
  count: number,
  options?: Partial<RandomCommitEventOptions>,
): IncompleteEvent[] {
  const opts: RandomCommitEventOptions = _.merge(
    {
      fromProbability: 1,
      repoNameGenerator: () => `repo-${randomUUID()}`,
      usernameGenerator: () => `user-${randomUUID()}`,
    },
    options,
  );

  const events: IncompleteEvent[] = [];

  for (let i = 0; i < count; i++) {
    const randomToRepoName = opts.repoNameGenerator();
    const randomFromUsername = opts.usernameGenerator();
    const randomTime = DateTime.now()
      .minus({ days: 10 })
      .plus({ minutes: randomInt(14400) });
    const randomSourceId = randomUUID();
    const event: IncompleteEvent = {
      time: randomTime,
      type: {
        version: 1,
        name: "COMMIT_CODE",
      },
      to: {
        type: ArtifactType.GIT_REPOSITORY,
        name: randomToRepoName,
        namespace: ArtifactNamespace.GITHUB,
      },
      sourceId: randomSourceId,
      amount: 1,
      details: {},
    };
    // probabilistically add a from field
    if (Math.random() > 1.0 - opts.fromProbability) {
      event.from = {
        type: ArtifactType.GITHUB_USER,
        name: randomFromUsername,
        namespace: ArtifactNamespace.GITHUB,
      };
    }
    events.push(event);
  }
  return events;
}
