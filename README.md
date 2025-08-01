# Flipside Simple API Query

This is a simple TypeScript project demonstrating basic API queries using Flipside.

## Project Structure

You can edit sql in the sql.ts

```
flipside-query
├── src
│   ├── index.ts
│   └── sql.ts
├── package.json
├── tsconfig.json
├── .prettierrc
├── .eslintrc.json
├── .gitignore
└── README.md
```

## Getting Started

To get started with this project, follow the steps below:

1. **Clone the repository**:

   ```sh
   git clone <repository-url>
   cd flipside-query
   ```

2. **Install dependencies**:

   ```sh
   yarn
   ```

3. **Set up environment variables**:  
   Create a `.env` file in the root directory and add your Flipside API key:

   ```
   FLIPSIDE_API_KEY=your_actual_api_key
   ```

4. **Compile the TypeScript files**:

   ```sh
   npx tsc
   ```

5. **Build & Run the application**:
   ```sh
   npm run start
   ```

## Features

- Basic TypeScript setup
- Prettier and ESLint integration for code quality and formatting
- Sample API query implementation in `src/index.ts`

## License

This project is licensed under the MIT License.
