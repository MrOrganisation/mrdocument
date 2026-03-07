# Context-Based Metadata Extraction

## Overview

The mrdocument service now supports context-based metadata extraction using a two-pass AI approach. This feature allows you to define different contexts (like "work", "private", "project-X") and provide context-specific instructions for how metadata should be extracted from documents.

## How It Works

### Two-Pass Extraction

When contexts are provided, the system performs two separate AI calls:

1. **First Pass - Context Determination**
   - The AI analyzes the document and determines which context it belongs to
   - It chooses from the list of provided context definitions
   - Decision is based on the context name and description

2. **Second Pass - Metadata Extraction**
   - The AI extracts metadata using context-specific instructions
   - Instructions can be provided for any metadata field:
     - `type_instructions`: How to determine document type
     - `sender_instructions`: How to determine sender
     - `topic_instructions`: How to determine topic
     - `subject_instructions`: How to determine subject
     - `keywords_instructions`: How to determine keywords

### Fallback Behavior

- If context determination fails, the system falls back to single-pass extraction
- If the AI returns an invalid context name, it defaults to the first context
- Context-specific instructions are optional - contexts can have just name and description

## Context Filter Support (Advanced)

Contexts can now include field-specific filters that override global filters during the second pass:

```json
{
  "name": "work",
  "description": "Work documents",
  "instructions": {
    "topic_instructions": "Use project code"
  },
  "type_filter": {
    "candidates": ["Invoice", "Receipt", "Contract"],
    "strict": true  // Optional: strict mode
  },
  "sender_filter": {
    "candidates": ["ACME Corp", "Globex Inc"],
    "blacklist": ["Unknown"]
  }
}
```

**Strict mode:** When a filter has `"strict": true`, the AI must choose from the candidates list only. No new values will be created.

**Note:** The Syncthing watcher automatically generates these filters from the user-friendly YAML configuration. See `watcher/FIELD_CONFIGURATION.md` for details.

## API Usage

### Context Definition Format

Each context must include:
- `name` (required): Unique identifier for the context
- `description` (required): Description to help AI determine if document belongs here
- `instructions` (optional): Object with field-specific instructions

```json
{
  "name": "work",
  "description": "Work-related documents from the company",
  "instructions": {
    "type_instructions": "Prefer specific document types like 'Purchase Order' over generic 'Document'",
    "sender_instructions": "Use the full company/department name, not individual names",
    "topic_instructions": "Use the project code or client name as the topic",
    "subject_instructions": "Keep subject concise, focus on the document purpose",
    "keywords_instructions": "Include technical terms and project-specific keywords"
  }
}
```

### Example Request

```bash
curl -X POST \
  -F "file=@document.pdf" \
  -F 'contexts=[
    {
      "name": "work",
      "description": "Work-related documents from the company, including invoices, contracts, and project documents",
      "instructions": {
        "topic_instructions": "Use the project code or client name",
        "sender_instructions": "Use the full company/department name"
      }
    },
    {
      "name": "private",
      "description": "Personal documents like utility bills, insurance documents, medical records, and bank statements",
      "instructions": {
        "topic_instructions": "Group by category: Utilities, Insurance, Medical, Banking, etc."
      }
    },
    {
      "name": "legal",
      "description": "Legal documents including court documents, legal correspondence, and contracts",
      "instructions": {
        "topic_instructions": "Use the case number or matter reference",
        "type_instructions": "Be specific about legal document types"
      }
    }
  ]' \
  http://localhost:8000/process
```

### Response Format

The response includes the determined context in the metadata:

```json
{
  "filename": "invoice-2024-01-15-acme_corp-project_alpha-order_12345.pdf",
  "pdf": "<base64-encoded-pdf>",
  "metadata": {
    "context": "work",
    "type": "Invoice",
    "date": "2024-01-15",
    "sender": "ACME Corporation",
    "topic": "Project Alpha",
    "subject": "Order 12345",
    "keywords": ["payment", "net30", "project-alpha"]
  }
}
```

## Use Cases

### 1. Work vs. Personal Document Management

Separate work documents from personal documents with different metadata extraction strategies:

- **Work**: Group by projects/clients, use company names
- **Private**: Group by categories (Utilities, Insurance), use service provider names

### 2. Multi-Project Document Organization

Handle documents from different projects with project-specific instructions:

- **Project A**: Use client code, include project-specific keywords
- **Project B**: Use department name, different categorization
- **Internal**: Use process type, administrative categories

### 3. Legal Document Management

Specialized extraction for different types of legal matters:

- **Litigation**: Use case numbers, court-specific metadata
- **Corporate**: Use matter codes, company entities
- **Real Estate**: Use property addresses, transaction types

### 4. Multi-User Scenarios

Different users with different document workflows:

- **User A**: Academic researcher (papers, grants, references)
- **User B**: Freelancer (invoices, contracts, clients)
- **User C**: Property manager (leases, maintenance, tenants)

## Implementation Details

### Configuration

Context-related prompts are defined in `config.yaml`:

- `context_prompt`: Template for context determination (first pass)
- `context_*_instruction`: Templates for context-specific instructions (second pass)

### Data Structures

```python
class ContextInstructions(TypedDict, total=False):
    type_instructions: str
    sender_instructions: str
    topic_instructions: str
    subject_instructions: str
    keywords_instructions: str

class ContextDefinition(TypedDict, total=False):
    name: str
    description: str
    instructions: ContextInstructions
```

### AI Client Methods

- `determine_context()`: First pass - determines document context
- `extract_metadata()`: Second pass - extracts metadata with context-specific instructions

## Best Practices

### Writing Good Context Descriptions

- Be specific and detailed
- Include examples of document types that belong to this context
- Mention distinguishing characteristics
- Consider what makes documents unique to this context

**Good Example:**
```
"Work-related documents from ACME Corporation, including purchase orders, 
invoices, project reports, meeting minutes, and internal correspondence. 
These documents typically contain company letterhead and project codes."
```

**Poor Example:**
```
"Work documents"
```

### Writing Effective Instructions

- Be explicit about what you want
- Provide examples when possible
- Consider edge cases
- Keep instructions concise but complete

**Good Example:**
```json
{
  "topic_instructions": "For project documents, use format 'Project [Code] - [Name]'. 
  For administrative documents, use the department name. For invoices, use 'Supplies' 
  or 'Services' as appropriate."
}
```

**Poor Example:**
```json
{
  "topic_instructions": "Determine the topic"
}
```

### Number of Contexts

- **2-3 contexts**: Optimal for most use cases
- **4-6 contexts**: Still manageable, but context determination may be less accurate
- **7+ contexts**: Consider if contexts can be merged or if sub-categorization would work better

### Context Overlap

- Avoid overlapping context definitions
- Each document should clearly belong to one context
- If contexts overlap, the AI may make unpredictable choices

## Performance Considerations

### API Calls

- Single-pass (without contexts): 1 AI call
- Two-pass (with contexts): 2 AI calls
- Cost and latency approximately doubles with contexts

### When to Use Contexts

**Use contexts when:**
- You have clearly distinct document categories
- Different categories need different extraction strategies
- Organization/categorization is important for your workflow

**Don't use contexts when:**
- All documents are similar in nature
- Simple filters (types, senders, topics) are sufficient
- API call cost/latency is a concern
- You only have one type of document

## Troubleshooting

### Context Not Determined Correctly

- Review context descriptions - make them more specific
- Check for overlapping contexts
- Verify the document actually fits one of the contexts
- Consider adding more distinguishing details to descriptions

### Instructions Not Being Followed

- Make instructions more explicit
- Check for conflicts between context instructions and global filters
- Verify instruction field names are correct
- Test with simpler instructions first

### Performance Issues

- Reduce number of contexts if possible
- Simplify context descriptions
- Cache context definitions client-side
- Consider if single-pass extraction would suffice

## Examples

See `README.md` for complete Python and curl examples demonstrating context usage.
