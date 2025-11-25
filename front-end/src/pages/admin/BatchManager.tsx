import React, { useEffect, useState } from 'react';
import { 
  Table, Button, Modal, Form, Input, DatePicker, message, 
  Popconfirm, Tag, Card 
} from 'antd';
import { 
  DeleteOutlined, PlusOutlined, ReloadOutlined, ClockCircleOutlined 
} from '@ant-design/icons';
import dayjs from 'dayjs';

// 引入生成的 Service 和类型
import { SelectionBatchApiService } from '../../client';
import type { SelectionBatchResp, SelectionBatchCreateParams } from '../../client';

export const BatchManager: React.FC = () => {
  const [batches, setBatches] = useState<SelectionBatchResp[]>([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  
  const [form] = Form.useForm();

  // === 1. 获取列表 ===
  const fetchBatches = async () => {
    setLoading(true);
    try {
      const res = await SelectionBatchApiService.getSelectionBatchApiV1SelectionBatchesGet();
      setBatches(res.result);
    } catch (error) {
      message.error('获取批次列表失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchBatches(); }, []);

  // === 2. 删除批次 ===
  const handleDelete = async (id: number) => {
    try {
      await SelectionBatchApiService.deleteSelectionBatchApiV1SelectionBatchesBatchIdDelete(id);
      message.success('删除成功');
      fetchBatches();
    } catch (error: any) {
      message.error(error.body?.detail || '删除失败');
    }
  };

  // === 3. 创建批次 ===
  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      // Antd RangePicker 返回的是 dayjs 对象，需要转成 ISO 字符串发给后端
      const createData: SelectionBatchCreateParams = {
        name: values.name,
        begin_time: values.timeRange[0].toISOString(),
        end_time: values.timeRange[1].toISOString()
      };
      
      await SelectionBatchApiService.createSelectionBatchApiV1SelectionBatchesPost(createData);
      message.success('创建成功');
      setIsModalOpen(false);
      form.resetFields();
      fetchBatches();
    } catch (error: any) {
      console.error(error);
      message.error(error.body?.detail || '创建失败');
    }
  };

  const formatTime = (t: string) => {
    if (!t) return '-';
    // 如果后端返回的时间字符串没有 'Z' 后缀（表示UTC），且没有时区偏移（+08:00），
    // 我们就默认它是 UTC 时间，手动加上 'Z' 让 dayjs 正确解析。
    const timeStr = (t.endsWith('Z') || t.includes('+')) ? t : t + 'Z';
    return dayjs(timeStr).format('YYYY-MM-DD HH:mm:ss');
  };

  const columns = [
    { title: 'ID', dataIndex: 'batch_id', width: 80 },
    { title: '批次名称', dataIndex: 'name', width: 200 },
    { 
      title: '开始时间', 
      dataIndex: 'begin_time', 
      render: formatTime
    },
    { 
      title: '结束时间', 
      dataIndex: 'end_time',
      render: formatTime
    },
    { 
      title: '状态', 
      dataIndex: 'status',
      width: 100,
      render: (status: string) => {
        const config: Record<string, { color: string, text: string }> = { 
          past: { color: 'default', text: '已结束' }, 
          current: { color: 'processing', text: '进行中' }, 
          future: { color: 'success', text: '未开始' } 
        };
        const conf = config[status] || { color: 'default', text: status };
        return <Tag color={conf.color}>{conf.text}</Tag>;
      }
    },
    {
      title: '操作',
      align: 'right' as const,
      render: (_: any, record: SelectionBatchResp) => (
        <Popconfirm 
          title="确定删除该选课批次？" 
          description="删除后学生将无法在该时段选课"
          onConfirm={() => handleDelete(record.batch_id)}
          okText="删除"
          cancelText="取消"
        >
          <Button danger icon={<DeleteOutlined />} size="small">删除</Button>
        </Popconfirm>
      )
    }
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16 }} bodyStyle={{ padding: '16px 24px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ fontWeight: 'bold', fontSize: 16 }}>
            <ClockCircleOutlined style={{ marginRight: 8 }} />
            选课批次管理
          </div>
          <div>
            <Button icon={<ReloadOutlined />} onClick={fetchBatches} style={{ marginRight: 8 }}>刷新</Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setIsModalOpen(true)}>
              新建批次
            </Button>
          </div>
        </div>
      </Card>

      <Table 
        dataSource={batches} 
        columns={columns} 
        rowKey="batch_id" 
        loading={loading} 
        pagination={false} // 批次一般不多，不需要分页
      />

      <Modal
        title="新建选课批次"
        open={isModalOpen}
        onOk={handleCreate}
        onCancel={() => setIsModalOpen(false)}
        okText="创建"
        cancelText="取消"
      >
        <Form form={form} layout="vertical">
          <Form.Item 
            name="name" 
            label="批次名称" 
            rules={[{ required: true, message: '请输入批次名称' }]}
          >
            <Input placeholder="例如：2025春季第一轮选课" />
          </Form.Item>
          
          <Form.Item 
            name="timeRange" 
            label="起止时间" 
            rules={[{ required: true, message: '请选择起止时间' }]}
          >
            <DatePicker.RangePicker 
              showTime 
              style={{ width: '100%' }} 
              placeholder={['开始时间', '结束时间']}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};