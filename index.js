import child_process from "node:child_process";

const server = child_process.spawn("node", ["src/server.js"], {
  stdio: "inherit",
});

server.on("exit", (code) => {
  console.log(`server exited with code ${code}`);
});