<div align="center">

<!-- Animated Header Banner -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,9,5,30&height=250&section=header&text=SchemeSaathi&fontSize=56&fontColor=ffffff&animation=fadeIn&fontAlignY=35&desc=Connecting%20Indian%20Citizens%20with%20Government%20Welfare%20Schemes&descSize=18&descAlignY=55&descColor=ffffff" width="100%"/>

<!-- Animated Typing SVG -->
<a href="https://readme-typing-svg.demolab.com/"><img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=600&size=20&pause=1000&color=FF6B35&center=true&vCenter=true&multiline=true&repeat=true&width=800&height=120&lines=AI-Powered+Government+Scheme+Finder+%F0%9F%8E%AF;Multilingual+Chatbot+%7C+Smart+Eligibility+Engine+%F0%9F%A4%96;Empowering+130+Crore+Indians+%F0%9F%87%AE%F0%9F%87%B3" alt="Typing SVG" /></a>

<br/>

<!-- Dynamic Badges Row 1 -->
[![Stars](https://img.shields.io/github/stars/prajwalzolage55/scheme_finder?style=for-the-badge&logo=github&logoColor=white&labelColor=0A1628&color=FF6B35)](https://github.com/prajwalzolage55/scheme_finder/stargazers)
[![Forks](https://img.shields.io/github/forks/prajwalzolage55/scheme_finder?style=for-the-badge&logo=github&logoColor=white&labelColor=0A1628&color=27AE60)](https://github.com/prajwalzolage55/scheme_finder/network/members)
[![Issues](https://img.shields.io/github/issues/prajwalzolage55/scheme_finder?style=for-the-badge&logo=github&logoColor=white&labelColor=0A1628&color=E74C3C)](https://github.com/prajwalzolage55/scheme_finder/issues)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge&logo=opensourceinitiative&logoColor=white&labelColor=0A1628&color=3498DB)](LICENSE)

<!-- Profile Views Counter -->
![Profile Views](https://komarev.com/ghpvc/?username=prajwalzolage55&label=Repository+Views&color=FF6B35&style=for-the-badge&abbreviated=true)
[![GitHub last commit](https://img.shields.io/github/last-commit/prajwalzolage55/scheme_finder?style=for-the-badge&logo=git&logoColor=white&labelColor=0A1628&color=9B59B6)](https://github.com/prajwalzolage55/scheme_finder/commits)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/prajwal-zolage-82ab10347)

<!-- Badges Row 2 — Tech Stack -->
<br/>

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.x-000000?style=for-the-badge&logo=flask&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-47A248?style=for-the-badge&logo=mongodb&logoColor=white)
![Groq](https://img.shields.io/badge/Groq-LLaMA_3.3-F55036?style=for-the-badge&logo=meta&logoColor=white)
![Selenium](https://img.shields.io/badge/Selenium-Scraper-43B02A?style=for-the-badge&logo=selenium&logoColor=white)
![Jinja2](https://img.shields.io/badge/Jinja2-Templates-B41717?style=for-the-badge&logo=jinja&logoColor=white)


</div>

---

<!-- Quick Navigation -->
<div align="center">

### 🧭 Quick Navigation

[✨ Features](#-features) · [🏗️ Architecture](#%EF%B8%8F-system-architecture) · [📦 Tech Stack](#-tech-stack) · [🚀 Getting Started](#-getting-started) · [📁 Project Structure](#-project-structure) · [🔌 API Docs](#-api-endpoints) · [🤖 AI Chatbot](#-ai-chatbot---schemesaathi) · [📊 Admin Panel](#-admin-panel) · [🛣️ Roadmap](#%EF%B8%8F-roadmap) · [🤝 Contributing](#-contributing)

</div>

---

## 📖 About The Project

> **SchemeSaathi** (Scheme + Saathi = Scheme Companion) is an AI-powered web platform that helps Indian citizens — especially low-income families — discover, understand, and apply for government welfare schemes they are eligible for.

Millions of rupees in welfare benefits go **unclaimed** every year because citizens don't know which schemes they qualify for. SchemeSaathi solves this by:

- 🎯 **Matching** users to schemes using a multi-criteria eligibility engine
- 🤖 **Answering** questions in **Hindi, Marathi & English** via an AI chatbot
- 📋 **Guiding** users through document checklists and step-by-step application processes
- 🔄 **Auto-fetching** real scheme data from [myscheme.gov.in](https://www.myscheme.gov.in) using Selenium

<div align="center">

```
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║   💡 "Don't Miss Schemes You Deserve"                             ║
║                                                                    ║
║   Ayushman Bharat • PM-KISAN • PMAY • MGNREGA • Ujjwala Yojana   ║
║   ... and 25+ more Central & Maharashtra State Schemes             ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
```

</div>

---

## ✨ Features

<table>
<tr>
<td width="50%">

### 🎯 Smart Eligibility Engine
Rule-based matching across **8 criteria**:
- 💰 Annual Income
- 🎂 Age Range
- 🚻 Gender
- 🏷️ Caste Category (SC/ST/OBC/General)
- 📍 State & Area Type (Rural/Urban)
- 💼 Occupation
- 📄 BPL Card Status
- 🏦 Bank Account

> Calculates a **match score (0-100%)** and returns eligible + exploratory schemes ranked by relevance.

</td>
<td width="50%">

### 🤖 AI Chatbot — SchemeSaathi
Powered by **Groq's LLaMA 3.3 70B** model:
- 🗣️ **Multilingual**: Hindi, Marathi, English
- 🎙️ **Voice Input**: Web Speech API for accessibility
- 👤 **Context-Aware**: Uses your profile data to personalize answers
- ⚡ **Sub-second responses** via Groq inference
- 🛡️ **Safe**: Never gives legal advice, suggests visiting CSC when unsure

</td>
</tr>
<tr>
<td width="50%">

### 🧾 Document Checklist & Guide
- ✅ Interactive checklists per scheme
- 📝 Step-by-step application guides
- 🔗 Direct links to official portals
- 📎 Know exactly what documents to carry

</td>
<td width="50%">

### 🕷️ Auto Web Scraper
- 🌐 Scrapes **myscheme.gov.in** using Selenium
- 🧠 Smart NLP extraction of eligibility rules
- 🔄 Scheduled auto-refresh (weekly)
- 📡 API interception from Chrome DevTools Protocol
- 🗃️ Upserts directly into MongoDB

</td>
</tr>
<tr>
<td width="50%">

### 🌗 Dark Mode & Responsive UI
- 🎨 Saffron + Navy Indian color theme
- 🌙 Toggle dark/light with persistence
- 📱 Mobile-first responsive design
- ⚡ Fast-loading, works on slow connections

</td>
<td width="50%">

### 🔐 Authentication & Admin
- 👤 User registration with hashed passwords
- 🔒 Session-based auth with Flask sessions
- 🛡️ Admin panel with API key protection
- 📊 Dashboard with database stats & actions

</td>
</tr>
</table>

---

## 🏗️ System Architecture

```mermaid
graph TB
    subgraph Client["🖥️ Client Browser"]
        UI[🎨 Jinja2 Templates]
        JS[⚡ JavaScript Modules]
        CSS[🎨 Custom CSS + Dark Mode]
    end

    subgraph Server["🐍 Flask Application"]
        APP[📦 app.py - Main Entry]
        AUTH[🔐 Auth Blueprint]
        SCHEMES[🎯 Schemes Blueprint]
        CHATBOT[🤖 Chatbot Blueprint]
        ADMIN[🛡️ Admin Blueprint]
        ENGINE[⚙️ Eligibility Engine]
    end

    subgraph Data["🗄️ Data Layer"]
        MONGO[(🍃 MongoDB Atlas)]
        JSON[📄 schemes_data.json]
    end

    subgraph External["🌐 External Services"]
        GROQ[🧠 Groq API - LLaMA 3.3]
        MYSCHEME[🇮🇳 myscheme.gov.in]
    end

    subgraph Tools["🔧 CLI Tools"]
        FETCH[🕷️ Selenium Scraper]
        SEED[🌱 DB Seeder]
        SCHED[⏰ Scheduler]
    end

    UI --> APP
    JS --> APP
    APP --> AUTH
    APP --> SCHEMES
    APP --> CHATBOT
    APP --> ADMIN

    SCHEMES --> ENGINE
    ENGINE --> MONGO
    CHATBOT --> GROQ
    AUTH --> MONGO
    ADMIN --> MONGO

    FETCH --> MYSCHEME
    FETCH --> MONGO
    SEED --> JSON
    SEED --> MONGO
    SCHED --> FETCH

    style Client fill:#1a1a2e,stroke:#FF6B35,color:#fff
    style Server fill:#0A1628,stroke:#FF6B35,color:#fff
    style Data fill:#16213e,stroke:#27AE60,color:#fff
    style External fill:#1a1a2e,stroke:#3498DB,color:#fff
    style Tools fill:#16213e,stroke:#9B59B6,color:#fff
```

### Data Flow

```mermaid
sequenceDiagram
    participant U as 👤 User
    participant F as 🐍 Flask
    participant E as ⚙️ Eligibility Engine
    participant M as 🍃 MongoDB
    participant G as 🧠 Groq AI

    U->>F: Fill eligibility form
    F->>M: Save search history
    F->>E: Pass user criteria
    E->>M: Load all schemes
    M-->>E: Return schemes list
    E->>E: Calculate match scores
    E-->>F: Eligible + Explore lists
    F-->>U: Render results page

    U->>F: Ask chatbot question
    F->>G: Send with user context
    G-->>F: AI response
    F-->>U: Display answer
```

---

## 📦 Tech Stack

<div align="center">

| Layer | Technology | Purpose |
|:---:|:---|:---|
| ![Python](https://img.shields.io/badge/-Python-3776AB?style=flat-square&logo=python&logoColor=white) | **Python 3.10+** | Core backend language |
| ![Flask](https://img.shields.io/badge/-Flask-000000?style=flat-square&logo=flask&logoColor=white) | **Flask** | Lightweight web framework with Blueprints |
| ![MongoDB](https://img.shields.io/badge/-MongoDB-47A248?style=flat-square&logo=mongodb&logoColor=white) | **MongoDB Atlas** | Cloud NoSQL database for schemes & users |
| ![Groq](https://img.shields.io/badge/-Groq-F55036?style=flat-square&logo=meta&logoColor=white) | **Groq (LLaMA 3.3 70B)** | Ultra-fast AI inference for chatbot |
| ![Selenium](https://img.shields.io/badge/-Selenium-43B02A?style=flat-square&logo=selenium&logoColor=white) | **Selenium + ChromeDriver** | Headless browser scraping |
| ![BS4](https://img.shields.io/badge/-BeautifulSoup4-8B0000?style=flat-square&logo=python&logoColor=white) | **BeautifulSoup4** | HTML parsing & data extraction |
| ![Jinja](https://img.shields.io/badge/-Jinja2-B41717?style=flat-square&logo=jinja&logoColor=white) | **Jinja2** | Server-side HTML templating |
| ![CSS](https://img.shields.io/badge/-CSS3-1572B6?style=flat-square&logo=css3&logoColor=white) | **Vanilla CSS** | Custom design system (31KB stylesheet) |
| ![JS](https://img.shields.io/badge/-JavaScript-F7DF1E?style=flat-square&logo=javascript&logoColor=black) | **JavaScript (ES6+)** | Chatbot UI, voice input, form validation |
| ![WebSpeech](https://img.shields.io/badge/-Web_Speech_API-4285F4?style=flat-square&logo=google&logoColor=white) | **Web Speech API** | Voice-to-text for accessibility |

</div>

---

## 🚀 Getting Started

### Prerequisites

<table>
<tr>
<td>

```
✅ Python 3.10 or higher
✅ MongoDB (local or Atlas)
✅ Google Chrome (for scraper)
✅ Groq API Key (free tier)
```

</td>
<td>

| Requirement | Get It |
|:---|:---|
| Python | [python.org](https://www.python.org/downloads/) |
| MongoDB | [mongodb.com/atlas](https://www.mongodb.com/atlas) |
| Groq API | [console.groq.com](https://console.groq.com/) |
| Chrome | [google.com/chrome](https://www.google.com/chrome/) |

</td>
</tr>
</table>

### ⚡ Quick Install

```bash
# 1️⃣ Clone the repository
git clone https://github.com/prajwalzolage55/scheme_finder.git
cd scheme-finder

# 2️⃣ Create virtual environment
python -m venv venv

# Activate — Windows
venv\Scripts\activate

# Activate — macOS/Linux
source venv/bin/activate

# 3️⃣ Install dependencies
pip install -r requirements.txt

# 4️⃣ Set up environment variables
copy .env.example .env
# Then edit .env with your keys (see below)

# 5️⃣ Seed the database
python tools/seed_db.py

# 6️⃣ Run the application
python app.py
```

### 🔑 Environment Variables

Create a `.env` file in the root directory:

```env
# 🔐 Flask
SECRET_KEY=your-super-secret-key-change-this

# 🍃 MongoDB Connection
MONGO_URI=mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/schemefinder?appName=Cluster0

# 🧠 Groq AI (Get free key at console.groq.com)
GROQ_API_KEY=gsk_your_groq_api_key_here

# 🐞 Debug Mode
FLASK_DEBUG=True

# 🛡️ Admin Panel Key
ADMIN_KEY=your-admin-key-here
```

> [!IMPORTANT]
> Never commit your `.env` file. It's already in `.gitignore`.

### 🎉 Access the App

After running `python app.py`, open your browser:

| Page | URL |
|:---|:---|
| 🏠 Home | [http://localhost:5000](http://localhost:5000) |
| 🔐 Login | [http://localhost:5000/login](http://localhost:5000/login) |
| 📝 Register | [http://localhost:5000/register](http://localhost:5000/register) |
| 🎯 Find Schemes | [http://localhost:5000/form](http://localhost:5000/form) |
| 🤖 AI Chatbot | [http://localhost:5000/chatbot](http://localhost:5000/chatbot) |
| 🛡️ Admin Panel | [http://localhost:5000/admin?key=YOUR_KEY](http://localhost:5000/admin?key=YOUR_KEY) |
| ℹ️ About | [http://localhost:5000/about](http://localhost:5000/about) |

---

## 📁 Project Structure

```
scheme-finder/
│
├── 📄 app.py                    # 🚀 Main Flask application entry point
├── 📄 config.py                 # ⚙️ Configuration (env vars, secrets)
├── 📄 requirements.txt          # 📦 Python dependencies
├── 📄 .env                      # 🔐 Environment variables (not committed)
├── 📄 .gitignore                # 🚫 Git ignore rules
│
├── 📂 routes/                   # 🛤️ Flask Blueprints (route handlers)
│   ├── auth.py                  #   🔐 Login, Register, Logout
│   ├── schemes.py               #   🎯 Form, Results, Detail, Checklist, Guide
│   ├── chatbot.py               #   🤖 AI Chatbot (Groq integration)
│   └── admin.py                 #   🛡️ Admin dashboard, fetch trigger, add scheme
│
├── 📂 engine/                   # ⚙️ Core Business Logic
│   └── eligibility.py           #   📊 Multi-criteria matching & scoring engine
│
├── 📂 models/                   # 🗃️ Data Models (MongoDB document shapes)
│   ├── user.py                  #   👤 User & profile schema
│   └── scheme.py                #   📋 Saved scheme schema
│
├── 📂 templates/                # 🎨 Jinja2 HTML Templates
│   ├── base.html                #   📐 Layout (navbar, footer, dark mode)
│   ├── home.html                #   🏠 Landing page with hero & features
│   ├── login.html               #   🔐 Login & Register (dual-mode)
│   ├── form.html                #   📝 Eligibility input form
│   ├── results.html             #   📊 Matched schemes dashboard
│   ├── scheme_detail.html       #   📄 Single scheme deep dive
│   ├── checklist.html           #   ✅ Document checklist
│   ├── guide.html               #   📖 Application step-by-step guide
│   ├── chatbot.html             #   🤖 AI chatbot interface
│   └── about.html               #   ℹ️ About page
│
├── 📂 static/                   # 📁 Static Assets
│   ├── css/
│   │   └── style.css            #   🎨 Complete design system (31KB)
│   └── js/
│       ├── chatbot.js           #   💬 Chat UI logic & API calls
│       ├── form.js              #   📝 Form validation & UX
│       └── voice.js             #   🎙️ Web Speech API integration
│
├── 📂 database/                 # 🗄️ Data Files
│   └── schemes_data.json        #   📊 Seed data (25+ schemes)
│
└── 📂 tools/                    # 🔧 CLI Utilities
    ├── fetch_schemes.py         #   🕷️ Selenium scraper for myscheme.gov.in
    ├── seed_db.py               #   🌱 One-time DB seeder
    └── scheduler.py             #   ⏰ Weekly auto-fetch scheduler
```

---

## 🔌 API Endpoints

### 🌐 Web Routes

| Method | Endpoint | Auth | Description |
|:---:|:---|:---:|:---|
| `GET` | `/` | ❌ | Landing page (redirects to `/home` if logged in) |
| `GET` | `/home` | ❌ | Home page with features showcase |
| `GET` | `/about` | ❌ | About page |
| `GET` | `/register` | ❌ | Registration page |
| `POST` | `/register` | ❌ | Create new account |
| `GET` | `/login` | ❌ | Login page |
| `POST` | `/login` | ❌ | Authenticate user |
| `GET` | `/logout` | ✅ | Clear session and logout |
| `GET` | `/form` | ✅ | Eligibility input form |
| `POST` | `/results` | ✅ | Process form → show matched schemes |
| `GET` | `/scheme/<id>` | ✅ | Scheme detail with match score |
| `GET` | `/checklist/<id>` | ✅ | Document checklist for a scheme |
| `GET` | `/guide/<id>` | ✅ | Step-by-step application guide |
| `GET` | `/chatbot` | ✅ | AI chatbot interface |

### 🤖 API Endpoints

| Method | Endpoint | Auth | Description |
|:---:|:---|:---:|:---|
| `POST` | `/api/chat` | ✅ | Send message to AI chatbot |

**Request:**
```json
{
  "messages": [
    { "role": "user", "content": "Am I eligible for PMAY?" }
  ]
}
```

**Response:**
```json
{
  "reply": "Based on your profile (income ₹1,50,000/year, age 35)..."
}
```

### 🛡️ Admin Endpoints

| Method | Endpoint | Auth | Description |
|:---:|:---|:---:|:---|
| `GET` | `/admin?key=KEY` | 🔑 | Admin dashboard with stats |
| `GET` | `/admin/fetch?key=KEY` | 🔑 | Trigger scheme scraper |
| `GET` | `/admin/add?key=KEY` | 🔑 | Manual scheme entry form |
| `POST` | `/admin/add` | 🔑 | Save manually added scheme |

---

## 🤖 AI Chatbot — SchemeSaathi

<div align="center">

```
╔═══════════════════════════════════════════════════════════╗
║                   🤖 SchemeSaathi AI                      ║
╠═══════════════════════════════════════════════════════════╣
║                                                           ║
║  👤 User: "Mujhe PMAY ke liye kaise apply karna hai?"    ║
║                                                           ║
║  🤖 Bot: "PMAY ke liye apply karne ke steps:             ║
║          1. PMAY website ya CSC centre jaiye              ║
║          2. Online form fill karein                        ║
║          3. Aadhaar, income certificate upload karein     ║
║          4. Reference number note karein                   ║
║          ..."                                              ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
```

</div>

### Capabilities

| Feature | Details |
|:---|:---|
| **Model** | Meta LLaMA 3.3 70B Versatile (via Groq) |
| **Languages** | Hindi 🇮🇳 · Marathi · English |
| **Context** | Auto-injects user profile (income, age, caste, state, etc.) |
| **Voice** | 🎙️ Web Speech API for voice-to-text input |
| **Safety** | Never gives legal advice; suggests visiting CSC when uncertain |
| **Speed** | Sub-second responses via Groq's LPU inference |

---

## 📊 Admin Panel

The admin panel provides a management dashboard accessible via API key:

```
http://localhost:5000/admin?key=YOUR_ADMIN_KEY
```

### Admin Features

- 📊 **Database Stats** — Total schemes, Central vs State breakdown, categories
- 🔄 **Fetch Triggers** — Pull new schemes from myscheme.gov.in (runs in background)
- 👩 **Category Fetch** — Target women schemes, Maharashtra schemes, etc.
- ➕ **Manual Entry** — Add schemes with full eligibility rules
- 📋 **Fetch Logs** — View last scraping results and timestamps
- 📜 **Scheme Table** — Browse all schemes with category, ministry, and eligibility rules

---

## ⚙️ Eligibility Engine — How Matching Works

The scoring algorithm in `engine/eligibility.py` evaluates users against scheme rules:

```
┌──────────────────────────────────────────────────────────────┐
│                    SCORING BREAKDOWN                          │
├──────────────────┬───────────────┬───────────────────────────┤
│ Criteria          │ Max Points    │ How It's Scored           │
├──────────────────┼───────────────┼───────────────────────────┤
│ 💰 Income        │ 30 pts        │ User income ≤ max_income  │
│ 🎂 Age           │ 20 pts        │ Within min_age — max_age  │
│ 🚻 Gender        │ 20 pts        │ Exact match               │
│ 🏷️ Caste        │ 20 pts        │ In allowed list           │
│ 📍 Area Type     │ 10 pts        │ Rural/Urban match         │
│ 💼 Occupation    │ 15 pts        │ In allowed occupations    │
│ 🗺️ State        │ 15 pts        │ State scheme match        │
│ 📄 BPL Card      │ 10 pts        │ Required = yes            │
│ 🏦 Bank Account  │  5 pts        │ Required = yes            │
├──────────────────┼───────────────┼───────────────────────────┤
│ TOTAL             │ Variable      │ Score = (earned/max)*100  │
└──────────────────┴───────────────┴───────────────────────────┘

   ✅ Score ≥ 50%  →  "Eligible" (shown first)
   🔍 Score < 50%  →  "Explore" (shown below)
   🚫 Has blockers & score > 40%  →  Capped at 25%
```

---

## 🕷️ Web Scraper Pipeline

The scraper (`tools/fetch_schemes.py`) follows this pipeline:

```mermaid
graph LR
    A[🌐 Launch Headless Chrome] --> B[📄 Load myscheme.gov.in]
    B --> C[📜 Scroll & Click 'Load More']
    C --> D{🔍 Try API Interception}
    D -->|Success| E[📡 Parse API JSON Response]
    D -->|Fail| F[🔗 Collect Scheme Links]
    F --> G[📄 Visit Each Detail Page]
    G --> H[🧠 Extract Name, Description, Ministry]
    E --> I[🏗️ Build Scheme Dict]
    H --> I
    I --> J[📊 NLP: Extract Eligibility Rules]
    J --> K[💾 Upsert into MongoDB]
```

### Extraction Intelligence

The scraper uses **regex-based NLP** to auto-extract eligibility rules from scheme descriptions:

| Extracted Field | Technique |
|:---|:---|
| Max Income | Pattern: `income.*less than.*₹X lakh` |
| Gender | Keywords: `women`, `female`, `mahila`, `widow` |
| Caste | Keywords: `scheduled caste`, `SC`, `OBC`, `tribal` |
| Area Type | Keywords: `rural`, `urban`, `village`, `city` |
| Occupation | Keyword mapping: `farmer`, `student`, `artisan`, etc. |
| Age Range | Pattern: `aged 18 to 40`, `above 60` |
| BPL | Keywords: `bpl`, `below poverty` |

---

## 🗃️ Database Schema

### Collections

```javascript
// 📋 schemes
{
  id: "pmay",                           // Unique slug
  name: "Pradhan Mantri Awas Yojana",
  category: "Housing",                  // Housing | Health | Education | ...
  ministry: "Ministry of Housing",
  description: "Provides affordable...",
  benefits: "Home loan subsidy up to...",
  eligibility: {
    max_income: 300000,
    area_type: ["rural", "urban"],
    gender: "female",                   // Optional
    caste: ["sc", "st", "obc"],        // Optional
    min_age: 18, max_age: 60,          // Optional
    bpl: true                           // Optional
  },
  documents: ["Aadhaar Card", "Income Certificate", ...],
  apply_link: "https://pmaymis.gov.in",
  apply_steps: ["Visit portal...", "Fill form...", ...],
  state: "central",                     // "central" or state name
  tags: ["housing", "subsidy", "loan"],
  source: "myscheme.gov.in",
  fetched_at: "2026-10-23T00:00:00"
}

// 👤 users
{
  name: "Prajwal",
  email: "user@email.com",
  phone: "9876543210",
  password: "<bcrypt_hash>",
  created_at: ISODate("2026-10-23"),
  profile_complete: false
}

// 🔍 searches
{
  user_id: "ObjectId(...)",
  user_data: { annual_income: 150000, age: 28, ... },
  timestamp: ISODate("2026-10-23")
}
```

---

## 🛠️ CLI Tools

### 🌱 Seed Database
```bash
# Load schemes_data.json into MongoDB (run once)
python tools/seed_db.py
```

### 🕷️ Fetch Schemes
```bash
# Fetch top 50 schemes
python tools/fetch_schemes.py

# Fetch 100 schemes
python tools/fetch_schemes.py --size 100

# Fetch women-specific schemes
python tools/fetch_schemes.py --query "women"

# Fetch Maharashtra state schemes
python tools/fetch_schemes.py --state maharashtra

# Preview without saving
python tools/fetch_schemes.py --preview
```

### ⏰ Auto Scheduler
```bash
# Run weekly auto-fetch (background daemon)
python tools/scheduler.py

# Run all fetch jobs once immediately
python tools/scheduler.py --now
```

**Scheduled Jobs:**
| Job | Query | Size | Description |
|:---|:---|:---:|:---|
| All Schemes | — | 100 | Top 100 from myscheme.gov.in |
| Women | `women` | 50 | Women-specific schemes |
| Agriculture | `farmer` | 50 | Farming & agriculture |
| Education | `scholarship` | 50 | Scholarships & education |
| Health | `health` | 50 | Healthcare schemes |
| Maharashtra | — (state filter) | 50 | MH state schemes |

---

## 🛣️ Roadmap

<div align="center">

```mermaid
timeline
    title SchemeSaathi Development Roadmap
    section Phase 1 - MVP ✅
        Core Features : Eligibility Engine
                      : User Auth (Register/Login)
                      : Scheme Results Dashboard
                      : Document Checklists
    section Phase 2 - AI ✅
        Intelligence  : Groq AI Chatbot
                      : Multilingual Support
                      : Voice Input
                      : Selenium Scraper
    section Phase 3 - Scale 🔜
        Growth        : WhatsApp Bot Integration
                      : SMS Notifications
                      : More State Schemes
                      : Android App (React Native)
    section Phase 4 - Impact 🎯
        Expansion     : Multi-language UI
                      : Offline Mode (PWA)
                      : Government API Integration
                      : Analytics Dashboard
```

</div>

- [x] ✅ Smart eligibility matching engine
- [x] ✅ User authentication (register/login/logout)
- [x] ✅ AI chatbot with Groq (Hindi/Marathi/English)
- [x] ✅ Voice input support
- [x] ✅ Selenium web scraper for myscheme.gov.in
- [x] ✅ Admin panel with fetch triggers
- [x] ✅ Dark mode with persistence
- [x] ✅ Document checklists & application guides
- [x] ✅ Mobile-responsive design
- [ ] 🔜 WhatsApp/Telegram bot
- [ ] 🔜 SMS alerts for new schemes
- [ ] 🔜 More state-level schemes (Gujarat, UP, Karnataka)
- [ ] 🔜 PWA for offline access
- [ ] 🔜 Hindi/Marathi UI translation
- [ ] 🔜 Government API integration (DigiLocker, Aadhaar)

---

## 🤝 Contributing

Contributions are what make the open-source community amazing! Any contributions you make are **greatly appreciated**.

<div align="center">

[![Contributors](https://img.shields.io/github/contributors/prajwalzolage55/scheme_finder?style=for-the-badge&logo=github&logoColor=white&labelColor=0A1628&color=FF6B35)](https://github.com/prajwalzolage55/scheme_finder/graphs/contributors)

</div>

```bash
# 1. Fork the repository
# 2. Create your feature branch
git checkout -b feature/AmazingFeature

# 3. Commit your changes
git commit -m "✨ Add AmazingFeature"

# 4. Push to the branch
git push origin feature/AmazingFeature

# 5. Open a Pull Request
```

### 💡 Ideas for Contributions

| Area | Ideas |
|:---|:---|
| 🌍 **i18n** | Add Hindi/Marathi UI translations |
| 📱 **Mobile** | Convert to PWA with service workers |
| 🗺️ **Schemes** | Add schemes for more states |
| 🧪 **Testing** | Add pytest unit tests for eligibility engine |
| 🎨 **UI** | Improve animations, add loading skeletons |
| 📊 **Analytics** | Build a user analytics dashboard |

---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for more information.

---

## 👨‍💻 Author

<div align="center">

<a href="https://github.com/prajwalzolage55">
  <img src="https://github.com/prajwalzolage55.png" width="120" style="border-radius:50%" alt="Prajwal Zolage"/>
</a>

### **Prajwal Ashok Zolage**

[![GitHub](https://img.shields.io/badge/GitHub-prajwalzolage55-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/prajwalzolage55)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Prajwal_Zolage-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/prajwal-zolage-82ab10347)

</div>

---

## 🙏 Acknowledgments

<div align="center">

| Resource | Purpose |
|:---:|:---|
| 🇮🇳 [myscheme.gov.in](https://www.myscheme.gov.in) | Official government scheme data source |
| 🧠 [Groq](https://groq.com) | Ultra-fast LLM inference (LLaMA 3.3 70B) |
| 🍃 [MongoDB Atlas](https://www.mongodb.com/atlas) | Cloud database hosting |
| 🐍 [Flask](https://flask.palletsprojects.com) | Lightweight Python web framework |
| 🤖 [Selenium](https://www.selenium.dev) | Browser automation for scraping |
| 🎨 [Shields.io](https://shields.io) | Beautiful README badges |
| ✨ [Capsule Render](https://github.com/kyechan99/capsule-render) | Dynamic header images |

</div>

---

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,9,5,30&height=120&section=footer&animation=fadeIn" width="100%"/>

**⭐ If SchemeSaathi helped you, give it a star! ⭐**

<br/>

[![Star this repo](https://img.shields.io/badge/⭐_Star_This_Repo-FF6B35?style=for-the-badge&logo=github&logoColor=white)](https://github.com/prajwalzolage55/scheme_finder)

*Made with ❤️ in India for India*

<br/>

![Wave](https://raw.githubusercontent.com/mayhemantt/mayhemantt/Update/svg/Bottom.svg)

</div>