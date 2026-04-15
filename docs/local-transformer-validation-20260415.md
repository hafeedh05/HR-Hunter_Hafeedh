# Local Transformer Validation — 2026-04-15

This note captures the current local transformer-first validation state from:

- Workspace: `C:\Users\abdul\Desktop\HR Hunter\HR Hunter Clone`
- Local app: `http://127.0.0.1:8765`

## Current App Projects Kept

The local app project list was intentionally reduced to these five validation projects:

1. `UAE Supply Chain Manager`
2. `CEO Test`
3. `AI Engineer Test`
4. `Project Architect Test`
5. `Senior Accountant Test`

These are the projects the teammate should verify in deployment.

## Family Count

Current top-level transformer family count:

- `31`

Source:

- `src/hr_hunter_transformer/taxonomy_data.yaml`

## Saved App Run Baselines

These are the latest saved app-level transformer runs after the local quality pass.

### Supply Chain Manager

- project: `UAE Supply Chain Manager`
- run id: `supply-chain-manager-bc6c17eb`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `182`
- review: `118`
- reject: `0`
- query count: `88`
- raw found: `1065`

### Chief Executive Officer (CEO)

- project: `CEO Test`
- run id: `chief-executive-officer-(ceo)-3b07c7ce`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `36`
- review: `264`
- reject: `0`
- query count: `196`
- raw found: `2510`

### AI Engineer

- project: `AI Engineer Test`
- run id: `ai-engineer-59381f5f`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `78`
- review: `222`
- reject: `0`
- query count: `165`
- raw found: `1623`

### Project Architect

- project: `Project Architect Test`
- run id: `project-architect-6cbf2b70`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `136`
- review: `164`
- reject: `0`
- query count: `135`
- raw found: `2241`

### Senior Accountant

- project: `Senior Accountant Test`
- run id: `senior-accountant-978adb2f`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `182`
- review: `118`
- reject: `0`
- query count: `73`
- raw found: `1173`

## Exact Hunt Briefs Used

### Supply Chain Manager

- project name: `UAE Supply Chain Manager`
- client: `Demo`
- role title: `Supply Chain Manager`
- countries: `United Arab Emirates`
- cities: `Dubai`, `Abu Dhabi`, `Sharjah`, `Jebel Ali`
- target titles:
  - `Supply Chain Manager`
  - `Senior Supply Chain Manager`
  - `Supply Planning Manager`
  - `Demand Planning Manager`
- must-have keywords:
  - `S&OP`
  - `Demand Planning`
  - `Inventory Optimization`
  - `Logistics`
  - `ERP`
- nice-to-have keywords:
  - `Warehouse Operations`
  - `Fulfillment`
  - `3PL`
  - `OTIF`
  - `Procurement`
  - `IBP`
  - `Regional Distribution`
  - `SAP`
- industry keywords:
  - `retail`
  - `ecommerce`
  - `consumer goods`
  - `logistics`
  - `distribution`
- peer companies:
  - `Amazon`
  - `noon`
  - `Majid Al Futtaim`
  - `Landmark Group`
  - `talabat`
  - `Careem`
  - `Aramex`
  - `DHL`
  - `Unilever`
  - `Nestle`
- JD text:
  - `We are hiring a UAE-based Supply Chain Manager to lead planning, inventory, logistics, and fulfillment performance across a fast-moving retail and ecommerce network. The brief prioritizes candidates with strong public evidence of S&OP ownership, demand and supply planning, inventory optimization, ERP-led operations, and distribution or warehouse coordination in the UAE market. Experience scaling service levels across omnichannel retail, consumer goods, 3PL, or regional distribution environments is highly valuable.`

### Chief Executive Officer (CEO)

- project name: `CEO Test`
- client: `Marina Home Interiors`
- role title: `Chief Executive Officer (CEO)`
- countries:
  - `United Arab Emirates`
  - `Saudi Arabia`
  - `Kuwait`
  - `Qatar`
  - `Bahrain`
- cities:
  - `Dubai`
  - `Abu Dhabi`
  - `Riyadh`
  - `Jeddah`
  - `Doha`
- target titles:
  - `Chief Executive Officer`
  - `CEO`
  - `Managing Director`
  - `General Manager`
  - `Country Manager`
- must-have keywords:
  - `P&L`
  - `Strategy`
  - `Transformation`
  - `Board`
  - `Growth`
- nice-to-have keywords:
  - `Retail`
  - `Consumer`
  - `Omnichannel`
  - `Regional Expansion`
  - `Leadership`
- industry keywords:
  - `retail`
  - `consumer goods`
  - `home furnishings`
  - `ecommerce`
- company targets:
  - `Marina Home Interiors`
- peer companies:
  - `IKEA`
  - `Home Centre`
  - `Pan Emirates`
  - `Pottery Barn`
  - `West Elm`
  - `The One`
- JD text:
  - `Executive leadership search for a CEO to lead a premium retail and home interiors business across the GCC. The ideal profile shows public evidence of full P&L ownership, board or investor exposure, retail transformation, omnichannel growth, and regional commercial leadership in consumer or lifestyle businesses.`

### AI Engineer

- project name: `AI Engineer Test`
- client: `Transformer Validation`
- role title: `AI Engineer`
- countries:
  - `United Arab Emirates`
  - `Saudi Arabia`
  - `Qatar`
- cities:
  - `Dubai`
  - `Abu Dhabi`
  - `Riyadh`
  - `Doha`
- target titles:
  - `AI Engineer`
  - `Machine Learning Engineer`
  - `Applied AI Engineer`
  - `LLM Engineer`
- must-have keywords:
  - `Machine Learning`
  - `LLM`
  - `Python`
  - `RAG`
  - `MLOps`
- nice-to-have keywords:
  - `LangChain`
  - `Vector Database`
  - `Transformers`
  - `AWS`
  - `Azure`
- industry keywords:
  - `technology`
  - `ai`
  - `software`
  - `saas`
- peer companies:
  - `Microsoft`
  - `Google`
  - `OpenAI`
  - `Careem`
  - `G42`
  - `Presight`
  - `DataRobot`
- JD text:
  - `Search for an AI Engineer with strong public evidence of production machine learning, LLM application building, retrieval-augmented generation, Python engineering, and model deployment. GCC market experience is preferred but strong adjacent MENA profiles are acceptable.`

### Project Architect

- project name: `Project Architect Test`
- client: `Architecture Validation`
- role title: `Project Architect`
- countries:
  - `United Arab Emirates`
  - `Saudi Arabia`
- cities:
  - `Dubai`
  - `Abu Dhabi`
  - `Riyadh`
- target titles:
  - `Project Architect`
  - `Senior Architect`
  - `Design Manager`
  - `Architect`
- must-have keywords:
  - `Architecture`
  - `Revit`
  - `Design Management`
  - `AutoCAD`
  - `Fit-out`
- nice-to-have keywords:
  - `Mixed-use`
  - `Hospitality`
  - `Retail`
  - `Project Delivery`
- industry keywords:
  - `architecture`
  - `design`
  - `real estate`
  - `construction`
- peer companies:
  - `Gensler`
  - `Atkins`
  - `AECOM`
  - `Killa Design`
  - `Dar Al Handasah`
- JD text:
  - `Search for a Project Architect with strong public evidence of architectural design delivery, Revit or AutoCAD fluency, project coordination, and fit-out or mixed-use project experience in the GCC market.`

### Senior Accountant

- project name: `Senior Accountant Test`
- client: `Accounting Validation`
- role title: `Senior Accountant`
- countries:
  - `United Arab Emirates`
- cities:
  - `Dubai`
  - `Abu Dhabi`
  - `Sharjah`
- target titles:
  - `Senior Accountant`
  - `Accounting Manager`
  - `Chief Accountant`
- must-have keywords:
  - `Accounting`
  - `Month End`
  - `Financial Reporting`
  - `IFRS`
  - `ERP`
- nice-to-have keywords:
  - `SAP`
  - `Oracle`
  - `Audit`
  - `General Ledger`
  - `Reconciliation`
- industry keywords:
  - `retail`
  - `consumer goods`
  - `distribution`
  - `manufacturing`
- peer companies:
  - `Majid Al Futtaim`
  - `Landmark Group`
  - `Al Futtaim`
  - `Al Tayer`
  - `Unilever`
- JD text:
  - `Search for a Senior Accountant with strong public evidence of month-end close, financial reporting, IFRS, ERP-led accounting operations, reconciliations, and controllership-style ownership in UAE-based commercial businesses.`

## Notes On Comparison

- Matching these numbers exactly in another environment is not required.
- Similar results are acceptable if:
  - backend is `transformer_v2`
  - requested count `300` is returned
  - the verified/review mix is in the same general band
  - the run clearly uses transformer retrieval and verification

## Notes On Learning

The current local quality pass now learns from saved runs in a limited, practical way:

- family run history from saved reports is read from `output/search`
- that history is used to adjust:
  - query budgets
  - page depth
  - source-site budgets
  - family-term budgets
  - family verifier thresholds

This is not yet full recruiter-feedback training. The feedback DB still has too little label diversity for a real LambdaRank retrain.
