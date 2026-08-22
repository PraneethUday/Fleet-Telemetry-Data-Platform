/// <reference types="vite/client" />

// Declaring the variables we read means a typo like `VITE_API_BASE` is a
// compile error rather than a silent `undefined` that falls back to localhost
// in a production build.
interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_BACKEND_START_COMMAND?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
