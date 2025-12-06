# Phase 2 – Pain Negation and Symptom Extraction (Days took – 3)

Phase 2 enhances the clinical ETL pipeline by adding NLP logic for pain negation detection and multi-symptom extraction.
The goal is to move from simple keyword matching to meaningful clinical text understanding.

---

## What I Built in Phase 2

### 1. Pain Negation Logic

The system now correctly handles cases where the note mentions the word "pain" but the meaning is actually negative.

Handled negation patterns:
- no pain
- denies pain
- without pain
- pain free
- pain-free
- absence of pain

Rules applied:
- If text contains no "pain" → flag = 0
- If text contains "pain" but also any negation → flag = 0
- Otherwise (real pain) → flag = 1

---

### 2. Symptom Extraction

A dictionary-based approach is used to extract multiple symptoms from the clinical note.

Symptoms detected:
- fever
- cough
- vomiting
- headache
- nausea
- weakness
- fatigue
- dizziness
- swelling
- chest pain
- shortness of breath
- diarrhea
- abdominal pain
- leg pain
- back pain

Extracted symptoms are stored as a comma-separated string such as:
- fever, vomiting
- weakness
- chest pain, dizziness

---

### 3. Output Table Enhancement

Phase 2 adds a new column to store extracted symptoms.

SQL change:
ALTER TABLE clinical_notes_output
ADD COLUMN IF NOT EXISTS symptoms TEXT;

Final table schema:
- unique_id
- summary
- flag
- symptoms

---

## Difficulties Faced in Phase 2

### 1. Column mismatch errors in PostgreSQL
When introducing the new `symptoms` column, the existing table did not contain it.
This caused repeated failures in the `write_results_atomic` step with errors like:
"column symptoms does not exist".

Fix:  
Manually altered the table using SQL and also added the automatic `ALTER TABLE` command in the DAG.

---

### 2. Designing a safe negation detection system
Pain negation is more complex than simple keyword matching.
Examples like:
- "no chest pain but abdominal pain present"
- "denies pain except mild back pain"
were initially being misclassified.

Fix:  
Introduced rule-based logic where negations only cancel the specific pain phrase they modify.

---

### 3. Ensuring symptom extraction did not produce false positives
Some words overlapped or caused misinterpretation.
Example:
- “He is weak and coughing” → Should detect both symptoms.
- “Pain-free but coughing” → Should not mark pain.

Fix:  
Added case-insensitive matching, boundary checks, and cleaned negation logic before extraction.

---

### 4. DAG failures due to missing or stale XCom data
When modifying the operator structure, some tasks tried to pull XCom before data existed, causing crashes.

Fix:  
Revalidated XCom flow, ensured each step pushes/pulls correctly.

<img width="886" height="276" alt="image" src="https://github.com/user-attachments/assets/07e8026a-7a12-410a-ae2d-0e9e0f4bd301" />


---

### 5. Handling large notes and summarization consistency
Long notes sometimes created inconsistent summaries.

Fix:  
Used simple truncation to maintain predictable formatting, planned upgrade for Phase 3 (ML summarizer).

---

## Test Data Used for Phase 2

Example clinical notes:
"Patient reports severe chest pain and dizziness."
"Denies pain but has fever and vomiting."
"No pain noted. Mild weakness present."
"Sharp abdominal pain with nausea."
"Shortness of breath but no pain."

Expected Output Table:

unique_id | flag | symptoms  
1 | 1 | chest pain, dizziness  
2 | 0 | fever, vomiting  
3 | 0 | weakness  
4 | 1 | abdominal pain, nausea  
5 | 0 | shortness of breath  

---
<img width="1219" height="541" alt="image" src="https://github.com/user-attachments/assets/6c7a7f85-8daa-43f1-a73c-d0897e1a41cd" />


<img width="1739" height="585" alt="image" src="https://github.com/user-attachments/assets/a1f081f7-8f74-441d-a76f-a900ecdf2b90" />

## Phase 2 Outcome

- Accurate interpretation of pain with negation detection.
- Multi-symptom extraction implemented.
- Output becomes more clinically meaningful.
- No architectural changes required.
- System is now ready for Phase 3 (ML + embeddings).

---

## Phase 3 Preview

Phase 3 will include:
- Embeddings (Word2Vec, ClinicalBERT)
- Text similarity search
- Classification models
- RAG integration
- UI for search and exploration

---

## Phase 2 Completed

