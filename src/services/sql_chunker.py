"""SQL chunking strategy for handling long SQL statements."""

import logging
import os
import re
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Configurable thresholds
MAX_SQL_LENGTH = int(os.getenv("MAX_SQL_LENGTH", "8000"))
MAX_SQL_LINES = int(os.getenv("MAX_SQL_LINES", "200"))


@dataclass
class SQLChunk:
    """Represents a chunk of SQL for conversion."""
    
    chunk_type: str  # 'cte', 'main', 'union', 'insert', 'alter_view', 'statement'
    content: str
    name: Optional[str] = None  # CTE name or table name
    index: int = 0


class SQLChunker:
    """Split long SQL into manageable chunks for conversion."""
    
    def __init__(self, sql: str):
        self.original_sql = sql.strip()
        self.chunks: list[SQLChunk] = []
    
    def should_chunk(self) -> bool:
        """Determine if SQL needs to be chunked."""
        return (
            len(self.original_sql) > MAX_SQL_LENGTH or 
            self.original_sql.count('\n') > MAX_SQL_LINES
        )
    
    def analyze_and_chunk(self) -> list[SQLChunk]:
        """Analyze SQL structure and split into chunks."""
        sql = self.original_sql
        
        logger.info(f"[Chunker] Analyzing SQL: {len(sql)} chars, {sql.count(chr(10))} lines")
        
        # 1. Detect multiple statements
        if self._has_multiple_statements(sql):
            logger.info("[Chunker] Detected multiple statements")
            return self._chunk_by_statements(sql)
        
        # 2. Detect INSERT...SELECT pattern
        if self._is_insert_select(sql):
            logger.info("[Chunker] Detected INSERT...SELECT pattern")
            return self._chunk_insert_select(sql)
        
        # 3. Detect ALTER VIEW
        if self._is_alter_view(sql):
            logger.info("[Chunker] Detected ALTER VIEW")
            return self._chunk_alter_view(sql)
        
        # 4. Detect CTE (WITH clause)
        if self._has_cte(sql):
            logger.info("[Chunker] Detected CTE (WITH clause)")
            return self._chunk_by_cte(sql)
        
        # 5. Detect UNION
        if self._has_union(sql):
            logger.info("[Chunker] Detected UNION")
            return self._chunk_by_union(sql)
        
        # 6. No chunking pattern detected
        logger.info("[Chunker] No chunking pattern detected, returning as single chunk")
        return [SQLChunk(chunk_type='main', content=sql, index=0)]
    
    def _has_multiple_statements(self, sql: str) -> bool:
        """Check if SQL contains multiple statements."""
        clean = self._remove_string_literals(sql)
        statements = [s.strip() for s in clean.split(';') if s.strip()]
        return len(statements) > 1
    
    def _remove_string_literals(self, sql: str) -> str:
        """Remove string literals from SQL for pattern matching."""
        result = re.sub(r"'[^']*'", "''", sql)
        result = re.sub(r'"[^"]*"', '""', result)
        return result
    
    def _chunk_by_statements(self, sql: str) -> list[SQLChunk]:
        """Split by semicolons (multiple statements)."""
        chunks = []
        
        statements = self._split_by_semicolon(sql)
        
        for i, stmt in enumerate(statements):
            stmt = stmt.strip()
            if stmt:
                chunk_type = self._detect_statement_type(stmt)
                chunks.append(SQLChunk(
                    chunk_type=chunk_type,
                    content=stmt,
                    index=i
                ))
        
        logger.info(f"[Chunker] Split into {len(chunks)} statements")
        return chunks
    
    def _split_by_semicolon(self, sql: str) -> list[str]:
        """Split SQL by semicolons, respecting string literals."""
        statements = []
        current = []
        in_string = False
        string_char = None
        
        for char in sql:
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
            
            if char == ';' and not in_string:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)
        
        # Last statement
        stmt = ''.join(current).strip()
        if stmt:
            statements.append(stmt)
        
        return statements
    
    def _detect_statement_type(self, sql: str) -> str:
        """Detect the type of SQL statement."""
        upper = sql.upper().strip()
        if upper.startswith('WITH'):
            return 'cte_query'
        elif upper.startswith('INSERT'):
            return 'insert'
        elif upper.startswith('ALTER VIEW'):
            return 'alter_view'
        elif upper.startswith('CREATE'):
            return 'create'
        elif upper.startswith('SELECT'):
            return 'select'
        elif upper.startswith('USE'):
            return 'use'
        else:
            return 'other'
    
    def _is_insert_select(self, sql: str) -> bool:
        """Check if SQL is INSERT ... SELECT pattern."""
        upper = sql.upper().strip()
        return upper.startswith('INSERT') and 'SELECT' in upper
    
    def _is_alter_view(self, sql: str) -> bool:
        """Check if SQL is ALTER VIEW."""
        upper = sql.upper().strip()
        return upper.startswith('ALTER VIEW') or upper.startswith('ALTER\nVIEW')
    
    def _chunk_insert_select(self, sql: str) -> list[SQLChunk]:
        """Split INSERT ... SELECT into parts."""
        chunks = []
        
        # Extract INSERT and SELECT parts
        match = re.match(
            r'(INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?TABLE\s+\S+)\s+((?:WITH|SELECT).*)',
            sql, 
            re.IGNORECASE | re.DOTALL
        )
        
        if match:
            insert_part = match.group(1)
            select_part = match.group(2)
            
            # INSERT part
            chunks.append(SQLChunk(
                chunk_type='insert_header',
                content=insert_part,
                index=0
            ))
            
            # SELECT part (may contain CTE)
            if self._has_cte(select_part):
                cte_chunks = self._chunk_by_cte(select_part)
                for i, chunk in enumerate(cte_chunks):
                    chunk.index = i + 1
                    chunks.append(chunk)
            elif self._has_union(select_part):
                union_chunks = self._chunk_by_union(select_part)
                for i, chunk in enumerate(union_chunks):
                    chunk.index = i + 1
                    chunks.append(chunk)
            else:
                chunks.append(SQLChunk(
                    chunk_type='select',
                    content=select_part,
                    index=1
                ))
        else:
            chunks.append(SQLChunk(chunk_type='insert', content=sql, index=0))
        
        logger.info(f"[Chunker] Split INSERT...SELECT into {len(chunks)} chunks")
        return chunks
    
    def _chunk_alter_view(self, sql: str) -> list[SQLChunk]:
        """Split ALTER VIEW into parts."""
        chunks = []
        
        # ALTER VIEW view_name AS SELECT ...
        match = re.match(
            r'(ALTER\s+VIEW\s+\S+\s+AS)\s+(SELECT.*)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        
        if match:
            alter_part = match.group(1)
            select_part = match.group(2)
            
            chunks.append(SQLChunk(
                chunk_type='alter_view_header',
                content=alter_part,
                index=0
            ))
            
            chunks.append(SQLChunk(
                chunk_type='select',
                content=select_part,
                index=1
            ))
        else:
            chunks.append(SQLChunk(chunk_type='alter_view', content=sql, index=0))
        
        logger.info(f"[Chunker] Split ALTER VIEW into {len(chunks)} chunks")
        return chunks
    
    def _has_cte(self, sql: str) -> bool:
        """Check if SQL has CTE (WITH clause)."""
        return bool(re.match(r'\s*WITH\s+', sql, re.IGNORECASE))
    
    def _chunk_by_cte(self, sql: str) -> list[SQLChunk]:
        """Split SQL by CTE definitions."""
        chunks = []
        
        # Find content after WITH
        match = re.match(r'\s*WITH\s+(.*)', sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return [SQLChunk(chunk_type='main', content=sql, index=0)]
        
        after_with = match.group(1)
        
        # Parse CTE blocks
        
        for i, (name, definition) in enumerate(cte_blocks):
            chunks.append(SQLChunk(
                chunk_type='cte',
                content=definition,
                name=name,
                index=i
            ))
        
        # Main query
        if main_query:
            chunks.append(SQLChunk(
                chunk_type='main',
                content=main_query,
                index=len(cte_blocks)
            ))
        
        logger.info(f"[Chunker] Split CTE query into {len(chunks)} chunks ({len(cte_blocks)} CTEs)")
        return chunks
    
    def _parse_cte_and_main(self, sql: str) -> tuple[list[tuple[str, str]], str]:
        """Parse CTE definitions and main query."""
        cte_blocks = []
        remaining = sql
        
        while True:
            # Match CTE: name AS (...)
            match = re.match(r'\s*(\w+)\s+AS\s*\(', remaining, re.IGNORECASE)
            if not match:
                break
            
            name = match.group(1)
            start_paren = match.end() - 1
            
            # Find matching closing parenthesis
            end_paren = self._find_matching_paren(remaining, start_paren)
            if end_paren < 0:
                break
            
            definition = remaining[start_paren:end_paren + 1]
            cte_blocks.append((name, definition))
            
            # Move to next position
            remaining = remaining[end_paren + 1:].strip()
            
            # Check for comma (more CTEs)
            if remaining.startswith(','):
                remaining = remaining[1:].strip()
            else:
                break
        
        # Remaining part is main query
        main_query = remaining.strip()
        
        return cte_blocks, main_query
    
    def _find_matching_paren(self, sql: str, start: int) -> int:
        """Find the matching closing parenthesis."""
        depth = 0
        in_string = False
        string_char = None
        
        for i in range(start, len(sql)):
            char = sql[i]
            
            # Handle string literals
            if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                continue
            
            if in_string:
                continue
            
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    return i
        
        return -1
    
    def _has_union(self, sql: str) -> bool:
        """Check if SQL has UNION (outside of subqueries)."""
        # Simple check: remove parentheses content then check UNION
        clean = self._remove_parentheses_content(sql)
        return bool(re.search(r'\bUNION\s+(?:ALL\s+)?', clean, re.IGNORECASE))
    
    def _remove_parentheses_content(self, sql: str) -> str:
        """Remove content inside parentheses for pattern matching."""
        result = []
        depth = 0
        in_string = False
        string_char = None
        
        for char in sql:
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
                if depth == 0:
                    result.append(char)
            elif char == string_char and in_string:
                in_string = False
                if depth == 0:
                    result.append(char)
            elif not in_string:
                if char == '(':
                    depth += 1
                    if depth == 1:
                        result.append('()')
                elif char == ')':
                    depth -= 1
                elif depth == 0:
                    result.append(char)
        
        return ''.join(result)
    
    def _chunk_by_union(self, sql: str) -> list[SQLChunk]:
        """Split SQL by UNION/UNION ALL at top level."""
        chunks = []
        
        # Find top-level UNION positions
        union_positions = self._find_top_level_unions(sql)
        
        if not union_positions:
            return [SQLChunk(chunk_type='main', content=sql, index=0)]
        
        # Split
        prev_end = 0
        for i, (start, end, union_type) in enumerate(union_positions):
            part = sql[prev_end:start].strip()
            if part:
                chunk_type = 'union_first' if i == 0 else 'union_part'
                chunks.append(SQLChunk(
                    chunk_type=chunk_type,
                    content=part,
                    index=len(chunks)
                ))
            prev_end = end
        
        # Last part
        last_part = sql[prev_end:].strip()
        if last_part:
            chunks.append(SQLChunk(
                chunk_type='union_part',
                content=last_part,
                index=len(chunks)
            ))
        
        logger.info(f"[Chunker] Split UNION query into {len(chunks)} chunks")
        return chunks
    
    def _find_top_level_unions(self, sql: str) -> list[tuple[int, int, str]]:
        """Find positions of top-level UNION keywords."""
        positions = []
        depth = 0
        in_string = False
        string_char = None
        i = 0
        
        while i < len(sql):
            char = sql[i]
            
            # 处理字符串
            if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                i += 1
                continue
            
            if in_string:
                i += 1
                continue
            
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif depth == 0:
                # Check UNION
                remaining = sql[i:].upper()
                if remaining.startswith('UNION ALL'):
                    positions.append((i, i + 10, 'UNION ALL'))
                    i += 10
                    continue
                elif remaining.startswith('UNION'):
                    positions.append((i, i + 5, 'UNION'))
                    i += 5
                    continue
            
            i += 1
        
        return positions


class ChunkedConverter:
    """Convert SQL chunks and merge results."""
    
    def __init__(self, converter_func: Callable[[str], str]):
        """
        Args:
            converter_func: Function that converts a single SQL chunk.
        """
        self.converter_func = converter_func
    
    def convert_chunks(self, chunks: list[SQLChunk]) -> str:
        """Convert each chunk and merge results."""
        converted_parts = []
        
        for chunk in chunks:
            logger.info(f"[ChunkedConverter] Converting chunk {chunk.index}: {chunk.chunk_type}" + 
                       (f" ({chunk.name})" if chunk.name else ""))
            
            if chunk.chunk_type == 'insert_header':
                # Convert INSERT header
                converted = self._convert_insert_header(chunk.content)
            elif chunk.chunk_type == 'alter_view_header':
                # Convert ALTER VIEW header
                converted = self._convert_alter_view_header(chunk.content)
            elif chunk.chunk_type == 'use':
                # Skip USE statement
                logger.info(f"[ChunkedConverter] Skipping USE statement")
                continue
            else:
                # Normal conversion
                converted = self.converter_func(chunk.content)
            
            converted_parts.append({
                'type': chunk.chunk_type,
                'name': chunk.name,
                'content': converted,
                'index': chunk.index
            })
        
        # Merge results
        return self._merge_parts(converted_parts)
    
    def _convert_insert_header(self, insert_sql: str) -> str:
        """Convert INSERT header to CREATE OR REPLACE."""
        # INSERT OVERWRITE TABLE xxx -> CREATE OR REPLACE TABLE `xxx` AS
        match = re.match(
            r'INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?TABLE\s+(\S+)',
            insert_sql,
            re.IGNORECASE
        )
        if match:
            table_name = match.group(1)
            # Remove backticks
            table_name = table_name.strip('`')
            return f"CREATE OR REPLACE TABLE `{table_name}` AS"
        return insert_sql
    
    def _convert_alter_view_header(self, alter_sql: str) -> str:
        """Convert ALTER VIEW header to CREATE OR REPLACE VIEW."""
        # ALTER VIEW xxx AS -> CREATE OR REPLACE VIEW `xxx` AS
        match = re.match(
            r'ALTER\s+VIEW\s+(\S+)\s+AS',
            alter_sql,
            re.IGNORECASE
        )
        if match:
            view_name = match.group(1)
            view_name = view_name.strip('`')
            return f"CREATE OR REPLACE VIEW `{view_name}` AS"
        return alter_sql
    
    def _merge_parts(self, parts: list[dict]) -> str:
        """Merge converted parts back together."""
        if not parts:
            return ""
        
        if len(parts) == 1:
            return parts[0]['content']
        
        # Sort by index
        parts = sorted(parts, key=lambda x: x['index'])
        
        result_parts = []
        cte_parts = []
        main_parts = []
        
        for part in parts:
            ptype = part['type']
            content = part['content'].strip()
            
            if ptype == 'cte':
                cte_parts.append((part['name'], content))
            elif ptype in ('insert_header', 'alter_view_header'):
                # Place at the beginning
                result_parts.insert(0, content)
            elif ptype in ('main', 'select', 'cte_query'):
                main_parts.append(content)
            elif ptype == 'union_first':
                main_parts.append(content)
            elif ptype == 'union_part':
                main_parts.append(content)
            elif ptype == 'statement':
                # Standalone statement
                main_parts.append(content + ';')
            else:
                main_parts.append(content)
        
        # Build CTE part
        if cte_parts:
            cte_strs = []
            for i, (name, definition) in enumerate(cte_parts):
                if i == 0:
                    cte_strs.append(f"WITH {name} AS {definition}")
                else:
                    cte_strs.append(f", {name} AS {definition}")
            result_parts.append('\n'.join(cte_strs))
        
        # Add main query part
        if main_parts:
            # Check if UNION is needed
            has_union = any(p['type'].startswith('union') for p in parts)
            if has_union:
                result_parts.append('\nUNION ALL\n'.join(main_parts))
            else:
                result_parts.append('\n'.join(main_parts))
        
        return '\n'.join(result_parts)


def chunk_and_convert(sql: str, converter_func: Callable[[str], str]) -> tuple[str, bool]:
    """
    Main entry point for chunked conversion.
    
    Args:
        sql: The SQL to convert.
        converter_func: Function to convert a single SQL chunk.
        
    Returns:
        Tuple of (converted_sql, was_chunked).
    """
    chunker = SQLChunker(sql)
    
    if chunker.should_chunk():
        logger.info(f"[chunk_and_convert] SQL needs chunking: {len(sql)} chars")
        chunks = chunker.analyze_and_chunk()
        
        if len(chunks) > 1:
            logger.info(f"[chunk_and_convert] Processing {len(chunks)} chunks")
            converter = ChunkedConverter(converter_func)
            result = converter.convert_chunks(chunks)
            return result, True
    
    # No chunking needed
    logger.info("[chunk_and_convert] Converting as single chunk")
    return converter_func(sql), False
