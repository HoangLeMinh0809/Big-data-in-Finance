import { useState } from "react";
import RealtimeDashboard from "./pages/RealtimeDashboard";
import HistoricalDashboard from "./pages/HistoricalDashboard";
import "./index.css";

function App() {
  const [page, setPage] = useState("realtime");

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h2>AIS UI</h2>
        <button
          className={page === "realtime" ? "nav-btn active" : "nav-btn"}
          onClick={() => setPage("realtime")}
        >
          Realtime Dashboard
        </button>
        <button
          className={page === "history" ? "nav-btn active" : "nav-btn"}
          onClick={() => setPage("history")}
        >
          Historical Dashboard
        </button>
      </aside>

      <main className="main-content">
        {page === "realtime" ? <RealtimeDashboard /> : <HistoricalDashboard />}
      </main>
    </div>
  );
}

export default App;