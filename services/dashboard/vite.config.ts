import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the API runs from docker compose on :8000; in the image, nginx proxies /v1.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: { proxy: { "/v1": "http://localhost:8000" } },
});
