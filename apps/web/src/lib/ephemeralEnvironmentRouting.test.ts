import { expect, it } from "vite-plus/test";
import { ephemeralCloneRemoteUrl } from "./ephemeralEnvironmentRouting";

it.each([
  ["https://user:secret@git.example/team/repo.git", "https://git.example/team/repo.git"],
  [
    "http://token%40account@git.example:8080/team/repo.git",
    "http://git.example:8080/team/repo.git",
  ],
  ["git@git.example:team/repo.git", "git@git.example:team/repo.git"],
  ["ssh://git@git.example/team/repo.git", "ssh://git@git.example/team/repo.git"],
])("uses a safe overflow clone URL for %s", (remoteUrl, expected) => {
  expect(ephemeralCloneRemoteUrl(remoteUrl)).toBe(expected);
});
