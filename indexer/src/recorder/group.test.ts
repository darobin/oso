import { ArtifactGroupRecorder } from "./group.js";
import { IEventRecorderClient, IncompleteEvent } from "./types.js";
import { FakeRecorderClient, randomCommitEventsGenerator } from "./testing.js";
import { ArtifactNamespace, ArtifactType } from "../index.js";

describe("ArtifactGroupRecorder", () => {
  let recorderClient: IEventRecorderClient;
  let randomEventsForRepo0: IncompleteEvent[];
  let randomEventsForRepo1: IncompleteEvent[];

  beforeEach(() => {
    recorderClient = new FakeRecorderClient();
    randomEventsForRepo0 = randomCommitEventsGenerator(10, {
      repoNameGenerator: () => {
        return "repo0";
      },
    });

    randomEventsForRepo1 = randomCommitEventsGenerator(10, {
      repoNameGenerator: () => {
        return "repo1";
      },
    });
  });

  it("group recorder handles commits", async () => {
    const groupRecorder = new ArtifactGroupRecorder(recorderClient);

    await groupRecorder.record(randomEventsForRepo0[0]);
    await groupRecorder.record(randomEventsForRepo1[0]);

    const result = await groupRecorder.wait({
      name: "not-in-recorder",
      namespace: ArtifactNamespace.GITHUB,
      type: ArtifactType.GIT_REPOSITORY,
    });

    expect(result.errors.length).toEqual(0);
    expect(result.success.length).toEqual(0);
  });
});
