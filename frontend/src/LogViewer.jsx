import React, { useEffect, useState } from 'react';
import { Button, Card, Input, Progress, Table, Tag } from 'antd';
import { getLogs, getWsUrl, getLogStats } from './api';

const LogStats = () => {
  const [stats, setStats] = useState([]);
  const [limit, setLimit] = useState(0);

  const fetchStats = async () => {
    try {
      const res = await getLogStats();
      setStats(res.stats || []);
      setLimit(res.limit || 0);
    } catch (e) {
      return;
    }
  };

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 3000);
    return () => clearInterval(interval);
  }, []);

  const columns = [
    {
      title: "账号",
      dataIndex: "phone",
      key: "phone",
      width: 150
    },
    {
      title: "进度",
      key: "progress",
      render: (_, record) => {
        let percent = 0;
        if (limit > 0) {
          percent = Math.floor((record.success / limit) * 100);
        }
        return (
          <div style={{ width: 180 }}>
            <Progress percent={percent} size="small" status="active" strokeColor={percent > 100 ? '#52c41a' : undefined} />
          </div>
        );
      }
    },
    {
      title: "成功",
      dataIndex: "success",
      key: "success",
      render: val => <span style={{ color: '#52c41a', fontWeight: 'bold' }}>{val}</span>
    },
    {
      title: "失败",
      dataIndex: "failed",
      key: "failed",
      render: val => <span style={{ color: '#ff4d4f', fontWeight: 'bold' }}>{val}</span>
    },
    {
      title: "限制",
      key: "limit",
      render: (_, record) => `${record.success}/${limit}`
    }
  ];

  if (stats.length === 0) return null;

  return (
    <Card size="small" style={{ marginBottom: 0 }}>
      <Table
        dataSource={stats}
        columns={columns}
        rowKey="phone"
        pagination={false}
        size="small"
        scroll={{ y: 240 }}
      />
      <div style={{ textAlign: 'right', marginTop: 8, color: '#999', fontSize: 12 }}>
        更新于 {new Date().toLocaleTimeString()}
      </div>
    </Card>
  );
};

const LogViewer = ({ embedded, taskId }) => {
  const [logs, setLogs] = useState([]);
  const [filter, setFilter] = useState('');

  useEffect(() => {
    setLogs([]);
    const fetchLogs = async () => {
      try {
        const res = await getLogs(taskId);
        setLogs(res.items);
      } catch (e) {
        return;
      }
    };

    fetchLogs();

    const wsUrl = getWsUrl(taskId);
    const ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (taskId && data.task_id != taskId) return;
        setLogs(prev => [data, ...prev]);
      } catch (e) {
        return;
      }
    };

    return () => {
      ws.close();
    };
  }, [taskId]);

  const filteredLogs = logs.filter(log => {
    if (!filter) return true;
    return (
      (log.target && log.target.includes(filter)) ||
      (log.status && log.status.includes(filter)) ||
      (log.error && log.error.includes(filter)) ||
      (log.task_id && String(log.task_id).includes(filter))
    );
  });

  const columns = [
    { title: "ID", dataIndex: "task_id", key: "task_id", width: 80, render: id => <Tag>#{id}</Tag> },
    { title: "时间", dataIndex: "time", key: "time", width: 180 },
    { title: "目标", dataIndex: "target", key: "target" },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      render: status => {
        let color = status === 'success' ? 'green' : status === 'failed' ? 'red' : 'blue';
        let text = status === 'success' ? '成功' : status === 'failed' ? '失败' : status;
        return <Tag color={color}>{text}</Tag>;
      }
    },
    { title: "信息", dataIndex: "error", key: "error" },
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <LogStats />
      <Card title="实时日志" size={embedded ? "small" : "default"} extra={
        <div style={{ display: 'flex', gap: 8 }}>
          <Input.Search
            placeholder="搜索日志..."
            allowClear
            onSearch={val => setFilter(val)}
            onChange={e => setFilter(e.target.value)}
            style={{ width: 200 }}
          />
          <Button size="small" danger onClick={() => setLogs([])}>清空日志</Button>
        </div>
      }>
        <div style={{ maxHeight: embedded ? 400 : 'calc(100vh - 400px)', overflowY: 'auto' }}>
          <Table
            dataSource={filteredLogs}
            columns={columns}
            rowKey={(r) => r.id || Math.random()}
            pagination={false}
            size="small"
          />
        </div>
      </Card>
    </div>
  );
};

export default LogViewer;
