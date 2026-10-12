/**
 * JSONC (JSON with Comments) Parser
 * 
 * Supports:
 * - Single-line comments: // comment
 * - Multi-line comments: / * comment * / (without spaces)
 */

import { log } from '../logger.js';

/**
 * Parse JSON with Comments (JSONC)
 * Strips single-line (//) and multi-line (/* * /) comments before parsing
 * 
 * @param content - Raw JSONC content with comments
 * @returns Parsed JSON object
 */
export function parseJsonc<T>(content: string): T {
  // Remove single-line comments (// ...)
  const noSingleLine = content.replace(/\/\/.*$/gm, '');

  // Remove multi-line comments (/* ... */)
  const noComments = noSingleLine.replace(/\/\*[\s\S]*?\*\//g, '');

  // Remove trailing commas (", }" / ", ]") left behind by comment stripping
  // or handwritten JSONC
  const noTrailingCommas = noComments.replace(/,(\s*[}\]])/g, '$1');

  try {
    return JSON.parse(noTrailingCommas) as T;
  } catch (err) {
    log(`JSONC parse error: ${err}`);
    throw err;
  }
}
