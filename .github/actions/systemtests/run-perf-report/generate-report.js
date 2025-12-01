const fs = require('fs');
const path = require('path');

/**
 * Map agent name to architecture
 */
function getArchFromAgent(agentName) {
  if (agentName.includes('arm')) {
    return 'arm64';
  }
  return 'amd64';
}

/**
 * Detect artifact directories (performance-results-*)
 */
function findArtifactDirs(baseDir) {
  if (!fs.existsSync(baseDir)) {
    console.warn(`Performance directory not found: ${baseDir}`);
    return [];
  }

  const entries = fs.readdirSync(baseDir, { withFileTypes: true });
  const artifactDirs = entries
    .filter(entry => entry.isDirectory() && entry.name.startsWith('performance-results-'))
    .map(entry => ({
      name: entry.name,
      path: path.join(baseDir, entry.name),
      arch: getArchFromAgent(entry.name)
    }));

  return artifactDirs;
}

/**
 * Find the timestamped results directory within an artifact
 */
function findTimestampedResultsDir(baseDir) {
  if (!fs.existsSync(baseDir)) {
    console.warn(`Performance directory not found: ${baseDir}`);
    return null;
  }

  const entries = fs.readdirSync(baseDir, { withFileTypes: true });
  const timestampDirs = entries
    .filter(entry => entry.isDirectory())
    .map(entry => entry.name)
    .sort()
    .reverse();

  if (timestampDirs.length === 0) {
    console.warn(`No timestamp directories found in ${baseDir}`);
    return null;
  }

  return path.join(baseDir, timestampDirs[0]);
}

/**
 * Read markdown content from results-table.md file
 */
function readResultsMarkdown(componentDir) {
  const mdPath = path.join(componentDir, 'results-table.md');
  if (!fs.existsSync(mdPath)) {
    return null;
  }
  return fs.readFileSync(mdPath, 'utf8');
}

/**
 * Parse markdown content to extract use cases with their tables
 * Returns array of { useCase, config, header, rows }
 */
function parseMarkdownContent(content) {
  if (!content) return [];

  const lines = content.trim().split('\n');
  const useCases = [];
  let currentUseCase = null;
  let inConfig = false;
  let inResults = false;
  let configLines = [];
  let tableLines = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    if (line.startsWith('**Use Case:**')) {
      // Save previous use case if exists
      if (currentUseCase) {
        useCases.push(buildUseCaseData(currentUseCase, configLines, tableLines));
      }
      currentUseCase = line.replace('**Use Case:**', '').trim();
      inConfig = false;
      inResults = false;
      configLines = [];
      tableLines = [];
    } else if (line.startsWith('**Configuration:**')) {
      inConfig = true;
      inResults = false;
    } else if (line.startsWith('**Results:**')) {
      inConfig = false;
      inResults = true;
    } else if (inConfig && line.startsWith('- ')) {
      configLines.push(line.substring(2));
    } else if (inResults && line.startsWith('|')) {
      tableLines.push(line);
    }
  }

  // Save last use case
  if (currentUseCase) {
    useCases.push(buildUseCaseData(currentUseCase, configLines, tableLines));
  }

  return useCases;
}

/**
 * Build use case data object from parsed lines
 */
function buildUseCaseData(useCase, configLines, tableLines) {
  // Filter out separator lines and parse table
  const dataLines = tableLines.filter(line => !line.match(/^\|[-:]+\|/));

  let header = [];
  let rows = [];

  if (dataLines.length > 0) {
    header = dataLines[0].split('|').slice(1, -1).map(col => col.trim());
    for (let i = 1; i < dataLines.length; i++) {
      const values = dataLines[i].split('|').slice(1, -1).map(col => col.trim());
      if (values.length === header.length) {
        rows.push(values);
      }
    }
  }

  return {
    useCase,
    config: configLines,
    header,
    rows
  };
}

/**
 * Format timestamp from directory name (yyyy-MM-dd-HH-mm-ss) to readable format
 */
function formatTimestamp(timestamp) {
  const parts = timestamp.split('-');
  if (parts.length === 6) {
    const [year, month, day, hour, minute] = parts;
    return `${year}-${month}-${day} ${hour}:${minute}`;
  }
  return timestamp;
}

/**
 * Merge tables from multiple architectures
 * For multi-arch, combines columns with architecture suffixes
 */
function mergeArchTables(archResults) {
  const archList = Object.keys(archResults).sort();

  if (archList.length === 1) {
    // Single architecture - return as-is with proper formatting
    const data = archResults[archList[0]];
    return {
      header: data.header,
      rows: data.rows,
      merged: false
    };
  }

  // Multi-architecture merge
  const firstArch = archList[0];
  const baseData = archResults[firstArch];

  // Identify identifier columns (# or columns that don't have metrics)
  // Heuristic: first column is usually row number, columns with "IN:" are identifiers
  const identifierIndices = [];
  const metricIndices = [];

  baseData.header.forEach((col, idx) => {
    if (col === '#' || col.startsWith('IN:')) {
      identifierIndices.push(idx);
    } else {
      metricIndices.push(idx);
    }
  });

  // Build merged header
  const mergedHeader = identifierIndices.map(idx => baseData.header[idx]);
  metricIndices.forEach(idx => {
    archList.forEach(arch => {
      mergedHeader.push(`${baseData.header[idx]} [${arch.toUpperCase()}]`);
    });
  });

  // Build merged rows
  const mergedRows = [];
  for (let rowIdx = 0; rowIdx < baseData.rows.length; rowIdx++) {
    const newRow = identifierIndices.map(idx => baseData.rows[rowIdx][idx]);

    metricIndices.forEach(idx => {
      archList.forEach(arch => {
        const archData = archResults[arch];
        const value = archData.rows[rowIdx]?.[idx] || 'N/A';
        newRow.push(value);
      });
    });

    mergedRows.push(newRow);
  }

  return {
    header: mergedHeader,
    rows: mergedRows,
    merged: true
  };
}

/**
 * Generate markdown table from header and rows
 */
function generateMarkdownTable(header, rows) {
  const lines = [];

  // Header row
  lines.push('| ' + header.join(' | ') + ' |');

  // Separator row
  lines.push('|' + header.map(() => '---').join('|') + '|');

  // Data rows
  rows.forEach(row => {
    lines.push('| ' + row.join(' | ') + ' |');
  });

  return lines.join('\n');
}

/**
 * Generate markdown summary for multiple architectures
 */
function generateMarkdownSummary(allResults) {
  const lines = [];

  lines.push('## Performance Test Results');
  lines.push('');

  // Get timestamp
  const allArchs = Object.keys(allResults).sort();
  const timestamps = new Set();
  allArchs.forEach(arch => {
    if (allResults[arch].timestamp) {
      timestamps.add(allResults[arch].timestamp);
    }
  });

  if (timestamps.size > 0) {
    const timestamp = Array.from(timestamps)[0];
    lines.push(`**Test Run:** \`${formatTimestamp(timestamp)}\``);
    lines.push('');
  }

  // Collect all operators
  const operators = new Set();
  allArchs.forEach(arch => {
    Object.keys(allResults[arch].operators || {}).forEach(op => operators.add(op));
  });

  // Generate report for each operator
  for (const operatorName of operators) {
    const title = operatorName.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    lines.push(`## ${title}`);
    lines.push('');

    // Group results by use case across architectures
    const useCasesByArch = new Map();

    for (const arch of allArchs) {
      const operatorData = allResults[arch].operators?.[operatorName];
      if (operatorData?.useCases) {
        operatorData.useCases.forEach(uc => {
          if (!useCasesByArch.has(uc.useCase)) {
            useCasesByArch.set(uc.useCase, {});
          }
          useCasesByArch.get(uc.useCase)[arch] = uc;
        });
      }
    }

    // For multi-arch, skip use cases that don't have data from all architectures
    for (const [useCase, archData] of useCasesByArch) {
      const numArchsWithData = Object.keys(archData).length;
      if (allArchs.length > 1 && numArchsWithData < allArchs.length) {
        console.warn(`Skipping ${operatorName}/${useCase}: only ${numArchsWithData} of ${allArchs.length} architectures have data`);
        continue;
      }

      lines.push(`**Use Case:** ${useCase}`);
      lines.push('');

      // Add configuration from first architecture
      const firstArchData = Object.values(archData)[0];
      if (firstArchData.config && firstArchData.config.length > 0) {
        lines.push('**Configuration:**');
        firstArchData.config.forEach(cfg => lines.push(`- ${cfg}`));
        lines.push('');
      }

      lines.push('**Results:**');
      lines.push('');

      // Merge tables if multi-arch
      const tableData = mergeArchTables(archData);
      lines.push(generateMarkdownTable(tableData.header, tableData.rows));
      lines.push('');
    }

    if (useCasesByArch.size === 0) {
      lines.push('_No results available_');
      lines.push('');
    }
  }

  return lines.join('\n');
}

/**
 * Main function to generate performance report
 * @param {string} perfDir - Directory containing performance results
 * @param {object} core - GitHub Actions core object for logging
 * @returns {object} - Object with has_results, summary, and timestamp
 */
function generatePerformanceReport(perfDir, core) {
  try {
    const artifactDirs = findArtifactDirs(perfDir);

    let allResults = {};
    let hasResults = false;
    let commonTimestamp = '';

    if (artifactDirs.length > 0) {
      core.info(`Found ${artifactDirs.length} artifact directories`);

      for (const artifactDir of artifactDirs) {
        core.info(`Processing artifact: ${artifactDir.name} (${artifactDir.arch})`);

        const timestampedDir = findTimestampedResultsDir(artifactDir.path);
        if (!timestampedDir) {
          core.warning(`No results found in ${artifactDir.name}`);
          continue;
        }

        const timestamp = path.basename(timestampedDir);
        if (!commonTimestamp) {
          commonTimestamp = timestamp;
        }

        const results = {
          timestamp,
          operators: {}
        };

        // Parse topic-operator results
        const topicOpDir = path.join(timestampedDir, 'topic-operator');
        const topicOpMd = readResultsMarkdown(topicOpDir);
        if (topicOpMd) {
          results.operators['topic-operator'] = {
            useCases: parseMarkdownContent(topicOpMd)
          };
          hasResults = true;
        }

        // Parse user-operator results
        const userOpDir = path.join(timestampedDir, 'user-operator');
        const userOpMd = readResultsMarkdown(userOpDir);
        if (userOpMd) {
          results.operators['user-operator'] = {
            useCases: parseMarkdownContent(userOpMd)
          };
          hasResults = true;
        }

        allResults[artifactDir.arch] = results;
      }
    } else {
      // Fallback for single directory (backward compatibility)
      core.info('No artifact directories found, checking for direct results');
      const timestampedDir = findTimestampedResultsDir(perfDir);

      if (timestampedDir) {
        const timestamp = path.basename(timestampedDir);
        commonTimestamp = timestamp;
        core.info(`Found performance results: ${timestamp}`);

        const results = {
          timestamp,
          operators: {}
        };

        const topicOpDir = path.join(timestampedDir, 'topic-operator');
        const topicOpMd = readResultsMarkdown(topicOpDir);
        if (topicOpMd) {
          results.operators['topic-operator'] = {
            useCases: parseMarkdownContent(topicOpMd)
          };
          hasResults = true;
        }

        const userOpDir = path.join(timestampedDir, 'user-operator');
        const userOpMd = readResultsMarkdown(userOpDir);
        if (userOpMd) {
          results.operators['user-operator'] = {
            useCases: parseMarkdownContent(userOpMd)
          };
          hasResults = true;
        }

        allResults['amd64'] = results;
      }
    }

    if (!hasResults) {
      return {
        has_results: 'false',
        summary: '_No performance results found_',
        timestamp: ''
      };
    }

    const summary = generateMarkdownSummary(allResults);

    core.info('Performance report generated successfully');

    return {
      has_results: 'true',
      summary: summary,
      timestamp: commonTimestamp
    };

  } catch (error) {
    core.error(`Error generating performance report: ${error.message}`);
    return {
      has_results: 'false',
      summary: `_Error generating performance report: ${error.message}_`,
      timestamp: ''
    };
  }
}

module.exports = { generatePerformanceReport };