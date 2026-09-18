import { useState, useEffect, useCallback, useMemo } from "react";
import { addOrder, deleteOrders, deleteOutbox, getListenOutbox, getOrders, getOutbox } from "./Services/order.service";
import { useSockets } from "../Features/useSocket";
import {
  Radio,
  ChevronRight,
  Trash2,
} from "lucide-react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip, Legend);

const MAX_LOGS = 400;

const CHART_COLORS = {
  dbWrite:   "#4a7c59",
  outbox:    "#b07d3a",
  consumer:  "#3a6b8a",
  kafkaFail: "#b84c4c",
};

const TAILING_TOPIC = "Orders_3___Transactional_Log_Tailing";

export default function App() {
  const {
    orderSocket1,
    consumerSocket,
    orderSocket2,
    poller_1_ltu_Socket,
    poller_1_top_Socket,
    poller_2_ltu_Socket,
    poller_2_top_Socket,
  } = useSockets();

  const [activePoller, setActivePoller] = useState("poller1-transactional");

  const [logs, setLogs] = useState({
    "poller1-transactional": [],
    "poller2-transactional": [],
    "poller1-listen": [],
    "poller2-listen": [],
  });

  const addLog = (pollerKey, message, type = "INFO") => {
    const newLog = {
      id: Date.now() + Math.random(),
      message,
      type,
      timestamp: new Date().toLocaleTimeString(),
    };

    setLogs((prev) => ({
      ...prev,
      [pollerKey]: [newLog, ...prev[pollerKey]].slice(0, MAX_LOGS),
    }));
  };

  const clearLogs = (pollerKey) => {
    setLogs((prev) => ({ ...prev, [pollerKey]: [] }));
  };

  const pollerButtons = useMemo(
    () => [
      { key: "poller1-transactional", title: "Poller 1", subtitle: "Transactional Outbox", icon: Radio, iconBg: "bg-orange-100", iconColor: "text-orange-500" },
      { key: "poller2-transactional", title: "Poller 2", subtitle: "Transactional Outbox", icon: Radio, iconBg: "bg-orange-100", iconColor: "text-orange-500" },
      { key: "poller1-listen",        title: "Poller 1", subtitle: "Listen To Yourself",   icon: Radio, iconBg: "bg-green-100",  iconColor: "text-green-500"  },
      { key: "poller2-listen",        title: "Poller 2", subtitle: "Listen To Yourself",   icon: Radio, iconBg: "bg-green-100",  iconColor: "text-green-500"  },
    ],
    []
  );

  const [formData, setFormData] = useState({
    pattern: "outbox",
    customerName: "",
    productName: "",
    quantity: 1,
    batchSize: 1,
    failureRate: 0.2,
  });

  const [graphData, setGraphData] = useState([]);

  const [batchStats, setBatchStats] = useState({
    totalRequests: 0,
    dbWritesSucceeded: 0,
    kafkaPublishesSucceeded: 0,
    dbWritesFailed: 0,
    outboxQueued: 0,
    kafkaPublishesFailed: 0,
    relayPending: 0,
    consumer_consumed: 0,
    inconsistentRuns: 0,
  });

  const handleKafkaSuccess = useCallback(({ poller, data } = {}) => {
    const eventId = data?.id ?? "unknown";
    if (poller == "Poller_1_top") addLog("poller1-transactional", `Kafka Event Publish SUCCESS : ${eventId}`, "SUCCESS");
    else if (poller == "Poller_2_top") addLog("poller2-transactional", `Kafka Event Publish SUCCESS : ${eventId}`, "SUCCESS");
    else if (poller == "Poller_1_ltu") addLog("poller1-listen", `Kafka Event Publish SUCCESS : ${eventId}`, "SUCCESS");
    else if (poller == "Poller_2_ltu") addLog("poller2-listen", `Kafka Event Publish SUCCESS : ${eventId}`, "SUCCESS");
    setBatchStats((prev) => ({ ...prev, kafkaPublishesSucceeded: prev.kafkaPublishesSucceeded + 1 }));
  }, []);

  const handleKafkaFailure = useCallback(({ poller, data } = {}) => {
    const eventId = data?.id ?? "unknown";
    if (poller == "Poller_1_top") addLog("poller1-transactional", `Kafka Event Publish FAILURE : ${eventId}`, "FAILURE");
    else if (poller == "Poller_2_top") addLog("poller2-transactional", `Kafka Event Publish FAILURE : ${eventId}`, "FAILURE");
    else if (poller == "Poller_1_ltu") addLog("poller1-listen", `Kafka Event Publish FAILURE : ${eventId}`, "FAILURE");
    else if (poller == "Poller_2_ltu") addLog("poller2-listen", `Kafka Event Publish FAILURE : ${eventId}`, "FAILURE");
    setBatchStats((prev) => ({ ...prev, kafkaPublishesFailed: prev.kafkaPublishesFailed + 1 }));
  }, []);

  const handleOrderAdded = useCallback(() => {
    setBatchStats((prev) => ({ ...prev, dbWritesSucceeded: prev.dbWritesSucceeded + 1 }));
  }, []);

  const handleOutboxOrderAdded = useCallback(() => {
    setBatchStats((prev) => ({ ...prev, outboxQueued: prev.outboxQueued + 1 }));
  }, []);

  const handle_DB_write_fail = useCallback(() => {
    setBatchStats((prev) => ({ ...prev, dbWritesFailed: prev.dbWritesFailed + 1 }));
  }, []);

  const handle_Consumed = useCallback(({ topic }) => {
    setBatchStats((prev) => ({
      ...prev,
      kafkaPublishesSucceeded:
        topic === TAILING_TOPIC
          ? prev.kafkaPublishesSucceeded + 1
          : prev.kafkaPublishesSucceeded,
      consumer_consumed: prev.consumer_consumed + 1,
      relayPending:
        topic !== TAILING_TOPIC
          ? prev.relayPending - 1
          : prev.relayPending,
    }));
  }, []);

  const handleResetSimulation = () => {
    setBatchStats({
      totalRequests: 0,
      dbWritesSucceeded: 0,
      kafkaPublishesSucceeded: 0,
      dbWritesFailed: 0,
      outboxQueued: 0,
      kafkaPublishesFailed: 0,
      relayPending: 0,
      consumer_consumed: 0,
      inconsistentRuns: 0,
    });
    setGraphData([]);
  };

  const [dbData, setDbData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [activeTab, setActiveTab] = useState("");

  useEffect(() => {
    const attachPollerEvents = (socket) => {
      if (!socket) return;
      socket.on("kafka-success", handleKafkaSuccess);
      socket.on("kafka-failure", handleKafkaFailure);
    };

    const detachPollerEvents = (socket) => {
      if (!socket) return;
      socket.off("kafka-success", handleKafkaSuccess);
      socket.off("kafka-failure", handleKafkaFailure);
    };

    const attachOrderEvents = (socket) => {
      if (!socket) return;
      socket.on("order-added", handleOrderAdded);
      socket.on("outbox-order-added", handleOutboxOrderAdded);
      socket.on("order-add-failed", handle_DB_write_fail);
    };

    const detachOrderEvents = (socket) => {
      if (!socket) return;
      socket.off("order-added", handleOrderAdded);
      socket.off("outbox-order-added", handleOutboxOrderAdded);
      socket.off("order-add-failed", handle_DB_write_fail);
    };

    const setupPollerLogs = (socket, pollerKey) => {
      if (!socket) return;
      socket.on("poller-started", () => addLog(pollerKey, "Poller Started", "START"));
      socket.on("cycle-started", () => addLog(pollerKey, "Cycle Started", "CYCLE"));
      socket.on("locking-and-processing", ({ data }) =>
        addLog(pollerKey, `Locking And Processing\n  Event ID : ${data?.id}\n  Payload : ${data?.payload}`, "LOCKED_DATA")
      );
      socket.on("processing", ({ data }) =>
        addLog(pollerKey, `Processing Event\n  Event ID : ${data?.id}`, "PROCESS")
      );
      socket.on("cleanup-started", () => addLog(pollerKey, "Cleanup Started", "CLEANUP"));
    };

    attachOrderEvents(orderSocket1);
    attachOrderEvents(orderSocket2);
    if (consumerSocket) consumerSocket.on("consumer-event", handle_Consumed);

    setupPollerLogs(poller_1_ltu_Socket, "poller1-listen");
    setupPollerLogs(poller_2_ltu_Socket, "poller2-listen");
    setupPollerLogs(poller_1_top_Socket, "poller1-transactional");
    setupPollerLogs(poller_2_top_Socket, "poller2-transactional");

    attachPollerEvents(poller_1_ltu_Socket);
    attachPollerEvents(poller_1_top_Socket);
    attachPollerEvents(poller_2_ltu_Socket);
    attachPollerEvents(poller_2_top_Socket);

    return () => {
      detachOrderEvents(orderSocket1);
      detachOrderEvents(orderSocket2);
      if (consumerSocket) consumerSocket.off("consumer-event", handle_Consumed);

      detachPollerEvents(poller_1_ltu_Socket);
      detachPollerEvents(poller_1_top_Socket);
      detachPollerEvents(poller_2_ltu_Socket);
      detachPollerEvents(poller_2_top_Socket);

      [poller_1_ltu_Socket, poller_2_ltu_Socket, poller_1_top_Socket, poller_2_top_Socket].forEach((s) => {
        s?.removeAllListeners("poller-started");
        s?.removeAllListeners("cycle-started");
        s?.removeAllListeners("locking-and-processing");
        s?.removeAllListeners("processing");
        s?.removeAllListeners("cleanup-started");
      });
    };
  }, [
    orderSocket1, orderSocket2, consumerSocket,
    poller_1_ltu_Socket, poller_1_top_Socket,
    poller_2_ltu_Socket, poller_2_top_Socket,
    handleOrderAdded, handleOutboxOrderAdded,
    handle_DB_write_fail, handle_Consumed,
    handleKafkaFailure, handleKafkaSuccess,
  ]);

  useEffect(() => {
    const interval = setInterval(() => {
      setBatchStats((currentStats) => {
        setGraphData((prev) => {
          const next = [
            ...prev,
            {
              time: new Date().toLocaleTimeString(),
              dbWritesSucceeded: currentStats.dbWritesSucceeded,
              kafkaPublishesSucceeded: currentStats.kafkaPublishesSucceeded,
              kafkaPublishesFailed: currentStats.kafkaPublishesFailed,
              consumerConsumed: currentStats.consumer_consumed,
            },
          ];
          return next.length > 30 ? next.slice(-30) : next;
        });
        return currentStats;
      });
    }, 1000);

    return () => clearInterval(interval);
  }, []);

  const handleGetOrders = async () => {
    setIsLoading(true); setActiveTab("orders");
    try { setDbData(await getOrders()); } catch (e) { console.error(e); } finally { setIsLoading(false); }
  };

  const handleGetOutbox = async () => {
    setIsLoading(true); setActiveTab("outbox");
    try { setDbData(await getOutbox()); } catch (e) { console.error(e); } finally { setIsLoading(false); }
  };

  const handleGetListenOutbox = async () => {
    setIsLoading(true); setActiveTab("listen");
    try { setDbData(await getListenOutbox()); } catch (e) { console.error(e); } finally { setIsLoading(false); }
  };

  const handleInputChange = (e) => {
    const { id, value } = e.target;
    const parsedValue = id === "quantity" || id === "batchSize" || id === "failureRate" ? Number(value) : value;
    setFormData((prev) => ({ ...prev, [id]: parsedValue }));
  };

  const handleSendOrders = async (e) => {
    e.preventDefault();
    if (!formData.customerName || !formData.productName) return;
    if (await addOrder(formData)) {
      setBatchStats((prev) => ({
        ...prev,
        totalRequests: prev.totalRequests + formData.batchSize,
        relayPending: formData.pattern !== "tailing" ? prev.relayPending + formData.batchSize : prev.relayPending,
      }));
    }
  };

  const handleDeleteOrders = async () => { deleteOrders(); };
  const handleDeleteOutbox = async () => { deleteOutbox(); };

  // ── chart data & options (built from graphData state) ──
  const chartData = {
    labels: graphData.map((d) => d.time),
    datasets: [
      {
        label: "DB Writes ✓",
        data: graphData.map((d) => d.dbWritesSucceeded),
        borderColor: CHART_COLORS.dbWrite,
        backgroundColor: "rgba(74,124,89,0.12)",
        fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
      },
      {
        label: "Consumer Consumed",
        data: graphData.map((d) => d.consumerConsumed),
        borderColor: CHART_COLORS.consumer,
        backgroundColor: "rgba(58,107,138,0.10)",
        fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
      },
      {
        label: "Kafka Publishes ✓",
        data: graphData.map((d) => d.kafkaPublishesSucceeded),
        borderColor: CHART_COLORS.outbox,
        backgroundColor: "rgba(176,125,58,0.10)",
        fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
      },
      {
        label: "Kafka Failures",
        data: graphData.map((d) => d.kafkaPublishesFailed),
        borderColor: CHART_COLORS.kafkaFail,
        backgroundColor: "rgba(184,76,76,0.10)",
        fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    plugins: {
      legend: {
        labels: { color: "#6c6459", font: { size: 12 }, boxWidth: 10, usePointStyle: true },
      },
      tooltip: {
        backgroundColor: "rgba(255,250,243,0.97)",
        borderColor: "rgba(31,27,23,0.12)",
        borderWidth: 1,
        titleColor: "#1f1b17",
        bodyColor: "#6c6459",
        padding: 10,
      },
    },
    scales: {
      x: {
        ticks: { color: "#9a8f84", font: { size: 10 }, maxTicksLimit: 8 },
        grid: { display: false },
      },
      y: {
        ticks: { color: "#9a8f84", font: { size: 10 } },
        grid: { color: "rgba(31,27,23,0.06)" },
        beginAtZero: true,
      },
    },
  };

  return (
    <div className="min-h-screen text-[#e8e6e3] font-sans bg-gradient-to-b from-[#0f1115] to-[#171a1f] p-4 md:p-8">
      {/* background blobs */}
      <div className="fixed top-0 left-0 w-full h-full pointer-events-none overflow-hidden z-0">
        <div className="absolute top-0 left-0 w-[600px] h-[600px] bg-[#3b82f6] opacity-15 blur-[100px] rounded-full -translate-x-1/2 -translate-y-1/2" />
        <div className="absolute top-0 right-0 w-[500px] h-[500px] bg-[#8b5cf6] opacity-10 blur-[100px] rounded-full translate-x-1/3 -translate-y-1/3" />
      </div>

      <main className="relative z-10 w-full max-w-[1240px] mx-auto pb-24">
        {/* ── HERO ── */}
        <section className="grid grid-cols-1 lg:grid-cols-[2fr_1fr] gap-5 mb-20 items-stretch">
          <div className="self-end">
            <p className="m-0 mb-2.5 text-[0.8rem] tracking-[0.18em] uppercase text-[#d0592c]">
              Distributed Systems
            </p>
            <h1 className="m-0 text-4xl md:text-5xl lg:text-[4.7rem] font-bold leading-[0.95] max-w-[11ch] tracking-tight">
              EventSync
            </h1>
            <p className="mt-4 m-0 max-w-[62ch] text-[#6c6459] leading-[1.6]">
              Master eventual consistency by simulating the dual write problem.
              Compare brittle sequential operations against robust, event-driven
              solutions to see how modern systems maintain database and message
              broker alignment.
            </p>
          </div>

          <div className="p-6 bg-[rgba(255,250,243,0.88)] backdrop-blur-md rounded-[24px] border border-[#1f1b17]/10 shadow-[0_18px_44px_rgba(58,38,17,0.12)] grid gap-8 content-center">
            {[
              { num: "01", label: "Transactional Outbox Pattern", active: true },
              { num: "02", label: "Listen to Yourself Pattern", active: false },
              { num: "03", label: "Transactional Log Tailing Pattern", active: false },
            ].map(({ num, label, active }) => (
              <div
                key={num}
                className="group relative overflow-hidden px-4 py-3.5 rounded-[14px] bg-[#FEFAF4] border border-[#EBE3D3] flex items-center justify-between cursor-pointer hover:bg-[#FFFBF7] hover:-translate-y-[2px] hover:shadow-[0_6px_10px_-3px_rgba(184,134,11,0.08)] transition-all duration-300"
              >
                <div className="absolute right-3 -bottom-3 text-[3.5rem] font-black text-[#45382a]/[0.05] select-none pointer-events-none group-hover:scale-110 transition-transform duration-500">
                  {num}
                </div>
                <div className="relative z-10 flex items-center gap-3">
                  <div className={`w-[3px] rounded-full transition-all duration-300 ${active ? "h-5 bg-[#B45309]" : "h-5 bg-[#D9CAB3] group-hover:h-6 group-hover:bg-[#B45309]"}`} />
                  <strong className={`text-[0.95rem] tracking-wide transition-colors duration-300 ${active ? "font-bold text-[#1E1B17]" : "font-semibold text-[#5F5449] group-hover:text-[#1E1B17]"}`}>
                    {label}
                  </strong>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* ── FORM + STATS ── */}
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-5 mb-5">
          {/* form */}
          <section className="p-6 md:p-8 bg-[rgba(255,250,243,0.95)] backdrop-blur-md rounded-[24px] border border-[#1f1b17]/10 shadow-[0_18px_44px_rgba(58,38,17,0.08)]">
            <div className="mb-6">
              <h2 className="m-0 text-xl md:text-2xl font-bold text-[#1E1B17] tracking-wide">
                Send orders to Order_Services to start simulation
              </h2>
            </div>

            <form id="bulk-form" className="grid gap-5" onSubmit={handleSendOrders}>
              <label className="grid gap-2 text-[0.95rem] font-medium text-[#5F5449]">
                Simulation Pattern
                <select id="pattern" value={formData.pattern} onChange={handleInputChange} required className="w-full p-3.5 rounded-[14px] border border-[#1f1b17]/15 bg-white text-[#2C261F] focus:outline-none focus:border-[#C25124] focus:ring-1 focus:ring-[#C25124] appearance-none transition-colors cursor-pointer shadow-sm">
                  <option value="outbox">1. Transactional Outbox Pattern</option>
                  <option value="listen">2. Listen to Yourself Pattern</option>
                  <option value="tailing">3. Transactional Log Tailing Pattern</option>
                </select>
              </label>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                <label className="grid gap-2 text-[0.95rem] font-medium text-[#5F5449]">
                  Customer Name
                  <input id="customerName" type="text" value={formData.customerName} onChange={handleInputChange} placeholder="e.g. John Doe" required className="w-full p-3.5 rounded-[14px] border border-[#1f1b17]/15 bg-white text-[#2C261F] placeholder-[#A89887] focus:outline-none focus:border-[#C25124] focus:ring-1 focus:ring-[#C25124] transition-colors shadow-sm" />
                </label>
                <label className="grid gap-2 text-[0.95rem] font-medium text-[#5F5449]">
                  Product Name
                  <input id="productName" type="text" value={formData.productName} onChange={handleInputChange} placeholder="e.g. Mechanical Keyboard" required className="w-full p-3.5 rounded-[14px] border border-[#1f1b17]/15 bg-white text-[#2C261F] placeholder-[#A89887] focus:outline-none focus:border-[#C25124] focus:ring-1 focus:ring-[#C25124] transition-colors shadow-sm" />
                </label>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                <label className="grid gap-2 text-[0.95rem] font-medium text-[#5F5449]">
                  Quantity
                  <input id="quantity" type="number" min="1" value={formData.quantity} onChange={handleInputChange} required className="w-full p-3.5 rounded-[14px] border border-[#1f1b17]/15 bg-white text-[#2C261F] focus:outline-none focus:border-[#C25124] focus:ring-1 focus:ring-[#C25124] transition-colors shadow-sm" />
                </label>
                <label className="grid gap-2 text-[0.95rem] font-medium text-[#5F5449]">
                  No. of similar Requests to send
                  <input id="batchSize" type="number" min="1" value={formData.batchSize} onChange={handleInputChange} required className="w-full p-3.5 rounded-[14px] border border-[#1f1b17]/15 bg-white text-[#2C261F] focus:outline-none focus:border-[#C25124] focus:ring-1 focus:ring-[#C25124] transition-colors shadow-sm" />
                </label>
              </div>

              <label className="grid gap-2 text-[0.95rem] font-medium text-[#5F5449]">
                Failure Rate [0, 1)
                <input id="failureRate" type="number" step="0.01" min="0" max="0.99" value={formData.failureRate} onChange={handleInputChange} required className="w-full p-3.5 rounded-[14px] border border-[#1f1b17]/15 bg-white text-[#2C261F] focus:outline-none focus:border-[#C25124] focus:ring-1 focus:ring-[#C25124] transition-colors shadow-sm" />
              </label>

              <div className="text-[#6C6459] text-[0.85rem] leading-[1.5] p-3.5 rounded-[12px] bg-white/60 border border-dashed border-[#1f1b17]/15">
                Simulate the error in the system publishing events or dbwrite. (e.g., 0.2 = 20% chance of failure).
              </div>

              <div className="flex flex-col md:flex-row flex-wrap gap-3 mt-4 pt-4 border-t border-[#1f1b17]/5">
                <button type="submit" className="flex-1 md:flex-none border-0 rounded-full px-7 py-3.5 text-white bg-gradient-to-r from-[#C25124] to-[#D96B2B] hover:from-[#B0471F] hover:to-[#C25124] cursor-pointer shadow-[0_8px_20px_rgba(194,81,36,0.2)] font-semibold transition-all duration-300 hover:-translate-y-0.5">
                  Send Orders
                </button>
                <button type="button" onClick={handleDeleteOrders} className="flex-1 md:flex-none border border-[#1f1b17]/15 rounded-full px-7 py-3.5 text-[#45382A] bg-white hover:bg-[#F9F5EC] cursor-pointer shadow-sm font-semibold transition-all duration-300">
                  Delete All Orders
                </button>
                <button type="button" onClick={handleDeleteOutbox} className="flex-1 md:flex-none border border-red-200 rounded-full px-7 py-3.5 text-red-700 bg-red-50 hover:bg-red-100 cursor-pointer shadow-sm font-semibold transition-all duration-300">
                  Delete all Outbox Data
                </button>
              </div>
            </form>
          </section>

          {/* stats cards */}
          <section className="relative p-6 md:p-8 bg-[rgba(255,250,243,0.95)] backdrop-blur-md rounded-[24px] border border-[#1f1b17]/10 shadow-[0_18px_44px_rgba(58,38,17,0.08)]">
            <div className="flex flex-col md:flex-row md:items-start justify-between gap-4 mb-6">
              <div>
                <h2 className="m-0 text-xl md:text-2xl font-bold text-[#1E1B17] tracking-wide">Latest Batch Flow</h2>
                <p className="m-0 mt-2 text-[#6c6459] text-[0.95rem] leading-[1.5]">
                  These cards summarize the outcome. Numbers update as Orders get added to Database and Events get published to Message Broker.
                </p>
              </div>
              <button onClick={handleResetSimulation} className="shrink-0 border border-[#1f1b17]/15 rounded-full px-5 py-2.5 text-[#45382A] bg-white hover:bg-[#F9F5EC] cursor-pointer shadow-sm font-semibold transition-all duration-300 hover:-translate-y-0.5">
                Reset Simulation
              </button>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4">
              {[
                { label: "Total Requests",           value: batchStats.totalRequests,            bg: "bg-white",      border: "border-[#1f1b17]/10", labelColor: "text-[#6c6459]",  valueColor: "text-[#1E1B17]" },
                { label: "DB Writes Succeeded",      value: batchStats.dbWritesSucceeded,        bg: "bg-[#E8EFE5]",  border: "border-[#CFDAC9]",    labelColor: "text-[#45523E]",  valueColor: "text-[#2A3624]" },
                { label: "Kafka Publishes Succeeded",value: batchStats.kafkaPublishesSucceeded,  bg: "bg-[#F4EBE1]",  border: "border-[#E3D4C1]",    labelColor: "text-[#635545]",  valueColor: "text-[#3B2E1E]" },
                { label: "DB Writes Failed",         value: batchStats.dbWritesFailed,           bg: "bg-white",      border: "border-[#1f1b17]/10", labelColor: "text-[#6c6459]",  valueColor: "text-[#1E1B17]" },
                { label: "Outbox Queued",            value: batchStats.outboxQueued,             bg: "bg-[#E8EFE5]",  border: "border-[#CFDAC9]",    labelColor: "text-[#45523E]",  valueColor: "text-[#2A3624]" },
                { label: "Kafka Publishes Failed",   value: batchStats.kafkaPublishesFailed,     bg: "bg-[#F5E6E6]",  border: "border-[#EAC9C9]",    labelColor: "text-[#6D4C4C]",  valueColor: "text-[#4A2626]" },
                { label: "Relay Pending",            value: batchStats.relayPending,             bg: "bg-[#F4EBE1]",  border: "border-[#E3D4C1]",    labelColor: "text-[#635545]",  valueColor: "text-[#3B2E1E]" },
                { label: "Consumer Consumed",        value: batchStats.consumer_consumed,        bg: "bg-[#E8EFE5]",  border: "border-[#CFDAC9]",    labelColor: "text-[#45523E]",  valueColor: "text-[#2A3624]" },
                { label: "Inconsistent Runs",        value: batchStats.inconsistentRuns,         bg: "bg-[#F5E6E6]",  border: "border-[#EAC9C9]",    labelColor: "text-[#6D4C4C]",  valueColor: "text-[#4A2626]" },
              ].map(({ label, value, bg, border, labelColor, valueColor }) => (
                <div key={label} className={`p-5 rounded-[18px] ${bg} border ${border} shadow-sm flex flex-col justify-between`}>
                  <span className={`${labelColor} text-[0.95rem] font-medium mb-3`}>{label}</span>
                  <strong className={`text-4xl font-bold ${valueColor}`}>{value}</strong>
                </div>
              ))}
            </div>
          </section>
        </section>

        {/* ── GLOBAL STATISTICS + GRAPH ── */}
        <section className="mb-5 p-6 bg-[rgba(255,250,243,0.88)] backdrop-blur-md rounded-[24px] border border-[#1f1b17]/10 shadow-[0_18px_44px_rgba(58,38,17,0.12)]">
          <div className="mb-5">
            <h2 className="m-0 text-2xl font-bold">Global Statistics</h2>
            <p className="m-0 mt-2 text-[#6c6459] leading-[1.5]">
              Aggregate numbers across recent tracked runs in this server process. Updates every second from live socket events.
            </p>
          </div>

          {/* mini summary cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            {[
              { label: "DB Writes Succeeded", value: batchStats.dbWritesSucceeded,       bg: "bg-[#E8EFE5]", border: "border-[#CFDAC9]", labelColor: "text-[#45523E]", valueColor: "text-[#2A3624]", dot: CHART_COLORS.dbWrite   },
              { label: "Outbox Queued",        value: batchStats.outboxQueued,            bg: "bg-[#F4EBE1]", border: "border-[#E3D4C1]", labelColor: "text-[#635545]", valueColor: "text-[#3B2E1E]", dot: CHART_COLORS.outbox    },
              { label: "Consumer Consumed",    value: batchStats.consumer_consumed,       bg: "bg-[#E5ECF4]", border: "border-[#C9D5E3]", labelColor: "text-[#3E4D5E]", valueColor: "text-[#1E2C3A]", dot: CHART_COLORS.consumer  },
              { label: "Kafka Failures",       value: batchStats.kafkaPublishesFailed,    bg: "bg-[#F5E6E6]", border: "border-[#EAC9C9]", labelColor: "text-[#6D4C4C]", valueColor: "text-[#4A2626]", dot: CHART_COLORS.kafkaFail },
            ].map(({ label, value, bg, border, labelColor, valueColor, dot }) => (
              <div key={label} className={`p-4 rounded-[18px] ${bg} border ${border} shadow-sm flex flex-col justify-between`}>
                <div className="flex items-center gap-2 mb-2">
                  <span style={{ background: dot }} className="inline-block w-2.5 h-2.5 rounded-full shrink-0" />
                  <span className={`${labelColor} text-[0.82rem] font-semibold leading-tight`}>{label}</span>
                </div>
                <strong className={`text-3xl font-bold ${valueColor}`}>{value}</strong>
              </div>
            ))}
          </div>

          {/* area chart */}
          <div className="bg-white/55 rounded-[18px] border border-[#1f1b17]/7 p-5">
            <p className="m-0 mb-3 text-[11px] font-semibold tracking-[0.1em] uppercase text-[#9a8f84]">
              Live trend · last 30 snapshots (1 s interval)
            </p>
            {graphData.length <= 1 ? (
              <div className="flex items-center justify-center h-[220px] text-[#a8998a] text-sm font-mono">
                // Send orders to start seeing live trend data
              </div>
            ) : (
              <div style={{ height: 400 }}>
                <Line data={chartData} options={chartOptions} />
              </div>
            )}
          </div>
        </section>

        {/* ── DB DATA ── */}
        <section className="mb-5 p-6 md:p-8 bg-[rgba(255,250,243,0.95)] backdrop-blur-md rounded-[24px] border border-[#1f1b17]/10 shadow-[0_18px_44px_rgba(58,38,17,0.08)]">
          <div className="mb-6">
            <h2 className="m-0 text-xl md:text-2xl font-bold text-[#1E1B17] tracking-wide">Get Database Data</h2>
            <p className="m-0 mt-2 text-[#6C6459] text-[0.95rem] leading-[1.5]">
              Inspect raw records from OrderServices related to events and persistence.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {[
              { tab: "orders", label: "Get Orders",                   handler: handleGetOrders },
              { tab: "outbox", label: "Get Transactional Outbox",     handler: handleGetOutbox },
              { tab: "listen", label: "Get Listen To Yourself",       handler: handleGetListenOutbox },
            ].map(({ tab, label, handler }) => (
              <button key={tab} onClick={handler} disabled={isLoading}
                className={`px-5 py-3.5 rounded-[14px] font-semibold transition-all duration-300 shadow-sm border ${activeTab === tab ? "bg-[#F9F5EC] border-[#C25124] text-[#C25124]" : "bg-white border-[#1f1b17]/15 text-[#45382A] hover:bg-[#F9F5EC]"}`}>
                {isLoading && activeTab === tab ? "Loading..." : label}
              </button>
            ))}
          </div>

          <div className="mt-6 rounded-[16px] bg-[#2A241D] border border-[#3B3228] shadow-[inset_0_2px_10px_rgba(0,0,0,0.2)] overflow-hidden">
            <div className="px-4 py-2 bg-[#1E1B17] border-b border-[#3B3228] flex items-center justify-between">
              <span className="text-xs font-mono text-[#A89887] uppercase tracking-wider">Server Response Viewer</span>
              {dbData && <span className="text-xs font-mono text-[#78A65E]">Status: 200 OK</span>}
            </div>
            <div className="p-5 overflow-x-auto min-h-[250px] max-h-[400px]">
              {isLoading ? (
                <div className="flex h-full items-center justify-center text-[#A89887] font-mono text-sm animate-pulse">Fetching records from database...</div>
              ) : dbData ? (
                <pre className="m-0 text-[#EBE3D3] font-mono text-sm leading-relaxed whitespace-pre-wrap">{JSON.stringify(dbData, null, 2)}</pre>
              ) : (
                <div className="flex h-full items-center justify-center text-[#5F5449] font-mono text-sm">// Click a button above to execute query</div>
              )}
            </div>
          </div>
        </section>

        {/* ── POLLER LOGS ── */}
        <section className="p-6 bg-[rgba(255,250,243,0.88)] backdrop-blur-md rounded-[24px] border border-[#1f1b17]/10 shadow-[0_18px_44px_rgba(58,38,17,0.12)]">
          <div className="mb-6">
            <h2 className="m-0 text-3xl font-bold text-[#171411]">Recent Outbox Logs</h2>
            <p className="m-0 mt-2 text-[#6c6459] leading-[1.7] text-[15px]">
              These sections show cycle and logs of pollers attached to each event stream.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-5 mb-6">
            {pollerButtons.map((poller) => {
              const Icon = poller.icon;
              const isActive = activePoller === poller.key;
              return (
                <button key={poller.key} onClick={() => setActivePoller(poller.key)}
                  className={`group w-full text-left p-5 rounded-3xl border transition-all duration-300 ${isActive ? "bg-[#1f1b17] text-white border-[#1f1b17] shadow-xl scale-[1.01]" : "bg-white/80 hover:bg-white border-[#e7ddd2]"}`}>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className={`w-16 h-16 rounded-2xl flex items-center justify-center ${isActive ? "bg-white/10" : poller.iconBg}`}>
                        <Icon className={`w-8 h-8 ${isActive ? "text-white" : poller.iconColor}`} />
                      </div>
                      <div>
                        <h3 className="text-xl font-bold">{poller.title}</h3>
                        <p className={`mt-1 text-sm ${isActive ? "text-gray-300" : "text-[#6c6459]"}`}>{poller.subtitle}</p>
                      </div>
                    </div>
                    <ChevronRight className={`w-6 h-6 transition-transform group-hover:translate-x-1 ${isActive ? "text-white" : "text-[#8f8578]"}`} />
                  </div>
                </button>
              );
            })}
          </div>

          <div className="overflow-hidden rounded-[24px] border border-[#2b241d] bg-[#161311] shadow-2xl">
            <div className="flex items-center justify-between px-5 py-3 border-b border-[#2d241c] bg-[#1d1814]">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-red-400" />
                <div className="w-3 h-3 rounded-full bg-yellow-400" />
                <div className="w-3 h-3 rounded-full bg-green-400" />
              </div>
              <div className="text-xs tracking-[0.2em] uppercase text-[#8b7761]">Poller Service Logs</div>
              <button onClick={() => clearLogs(activePoller)} className="flex items-center gap-2 px-3 py-1.5 text-xs rounded-lg bg-red-500/10 hover:bg-red-500/20 text-red-300 transition">
                <Trash2 className="w-4 h-4" /> Clear
              </button>
            </div>

            <div className="h-[360px] overflow-y-auto px-5 py-4 font-mono text-sm">
              {logs[activePoller].length === 0 ? (
                <div className="text-center text-[#6f6255] mt-20">// Click a poller above to view logs</div>
              ) : (
                <div className="space-y-3">
                  {logs[activePoller].map((log) => {
                    const variants = {
                      START:       { border: "border-emerald-500/30", bg: "bg-emerald-500/10", badge: "bg-emerald-500/20 text-emerald-300", icon: "🟢" },
                      CYCLE:       { border: "border-blue-500/30",    bg: "bg-blue-500/10",    badge: "bg-blue-500/20 text-blue-300",       icon: "🔄" },
                      LOCKED_DATA: { border: "border-yellow-500/30",  bg: "bg-yellow-500/10",  badge: "bg-yellow-500/20 text-yellow-300",   icon: "🔒" },
                      PROCESS:     { border: "border-purple-500/30",  bg: "bg-purple-500/10",  badge: "bg-purple-500/20 text-purple-300",   icon: "⚙️" },
                      CLEANUP:     { border: "border-pink-500/30",    bg: "bg-pink-500/10",    badge: "bg-pink-500/20 text-pink-300",       icon: "🧹" },
                      ERROR:       { border: "border-red-500/30",     bg: "bg-red-500/10",     badge: "bg-red-500/20 text-red-300",         icon: "❌" },
                      SUCCESS:     { border: "border-lime-500/30",    bg: "bg-lime-500/10",    badge: "bg-lime-500/20 text-lime-300",       icon: "✅" },
                      FAILURE:     { border: "border-red-600/30",     bg: "bg-red-600/10",     badge: "bg-red-600/20 text-red-300",         icon: "💥" },
                      INFO:        { border: "border-green-500/30",   bg: "bg-green-500/10",   badge: "bg-green-500/20 text-green-300",     icon: "ℹ️" },
                    };
                    const style = variants[log.type] || variants.INFO;
                    return (
                      <div key={log.id} className={`rounded-2xl px-4 py-4 border backdrop-blur-sm shadow-lg transition-all duration-300 hover:scale-[1.01] ${style.bg} ${style.border}`}>
                        <div className="flex items-start justify-between mb-3">
                          <div className="flex items-center gap-3">
                            <div className="text-xl">{style.icon}</div>
                            <div>
                              <div className={`inline-flex items-center rounded-full px-3 py-1 text-[11px] font-bold tracking-wide ${style.badge}`}>
                                {log.type}
                              </div>
                            </div>
                          </div>
                          <span className="text-[11px] text-[#9f9488]">{log.timestamp}</span>
                        </div>
                        <pre className="whitespace-pre-wrap break-words text-[#f4ede6] text-sm leading-relaxed font-mono overflow-x-auto">
                          {log.message}
                        </pre>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}
