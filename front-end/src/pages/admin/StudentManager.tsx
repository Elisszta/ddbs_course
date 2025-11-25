import { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, message, Popconfirm, InputNumber, Space, Card, Row, Col } from 'antd';
import { EditOutlined, DeleteOutlined, SearchOutlined, ReloadOutlined, PlusOutlined } from '@ant-design/icons';
import { StudentService } from '../../client';
import type { StudentResp, StudentCreateParams, StudentUpdateParams } from '../../client';
import type { ColumnType } from 'antd/es/table';

export const StudentManager = () => {
  const [students, setStudents] = useState<StudentResp[]>([]);
  const [loading, setLoading] = useState(false);
  
  const [searchId, setSearchId] = useState<number | undefined>();
  const [searchName, setSearchName] = useState<string>('');

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isEditMode, setIsEditMode] = useState(false);
  const [currentId, setCurrentId] = useState<number | null>(null);
  
  const [form] = Form.useForm();

  const fetchStudents = async () => {
    setLoading(true);
    try {
      const res = await StudentService.searchStudentApiV1StudentsGet(
        searchId,   // id 参数
        searchName || undefined // name 参数 (空字符串转 undefined)
      );
      setStudents(res.result);
      
      // 如果搜了东西但没结果
      if (res.result.length === 0 && (searchId || searchName)) {
        message.info('未找到匹配的学生');
      }
    } catch (error) {
      message.error('获取数据失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchStudents(); }, []);

  const openAddModal = () => {
    setIsEditMode(false);
    setCurrentId(null);
    form.resetFields();
    setIsModalOpen(true);
  };

  const openEditModal = (record: StudentResp) => {
    setIsEditMode(true);
    setCurrentId(record.stu_id);
    form.setFieldsValue({
      year: undefined, 
      name: record.name,
      sex: record.sex,
      age: record.age,
      current_campus: record.current_campus,
    });
    setIsModalOpen(true);
  };

  const handleAdd = async () => {
    try {
      const values = await form.validateFields();
      await StudentService.addStudentApiV1StudentsPost(values as StudentCreateParams);
      message.success('添加成功');
      setIsModalOpen(false);
      fetchStudents();
    } catch (error) {
      message.error('添加失败');
    }
  };

  const handleUpdate = async () => {
    try {
      const values = await form.validateFields();
      if (currentId === null) return;

      const updateData: StudentUpdateParams = {
        name: values.name,
        sex: values.sex,
        age: values.age,
        current_campus: values.current_campus
      };

      await StudentService.updateStudentApiV1StudentsStudentIdPut(currentId, updateData);
      message.success('修改成功');
      setIsModalOpen(false);
      fetchStudents();
    } catch (error) {
      message.error('修改失败');
    }
  };

  const handleOk = () => {
    if (isEditMode) {
      handleUpdate();
    } else {
      handleAdd();
    }
  };

  const handleDelete = async (id: number) => {
    await StudentService.deleteStudentApiV1StudentsStudentIdDelete(id);
    message.success('删除成功');
    fetchStudents();
  };

  const columns = [
    { title: '学号', dataIndex: 'stu_id', width: 150 },
    { title: '姓名', dataIndex: 'name' },
    { title: '性别', dataIndex: 'sex', width: 80 },
    { title: '年龄', dataIndex: 'age', width: 80 },
    { title: '校区', dataIndex: 'current_campus', width: 100 },
    {
      title: '',
      align: 'right',
      render: (_: any, record: StudentResp) => (
        <Space>
          <Button icon={<EditOutlined />} onClick={() => openEditModal(record)}>
            编辑
          </Button>
          <Popconfirm title="确认删除?" onConfirm={() => handleDelete(record.stu_id)}>
            <Button danger type="link" icon={<DeleteOutlined />}>删除</Button>
          </Popconfirm>
        </Space>
      )
    }
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16 }} bodyStyle={{ padding: '16px 24px' }}>
        <Row gutter={16} align="middle">
          <Col>
            <InputNumber 
              placeholder="搜索学号 (精确)" 
              style={{ width: 180 }} 
              value={searchId}
              onChange={(val) => setSearchId(val ?? undefined)}
              onPressEnter={fetchStudents}
            />
          </Col>
          <Col>
            <Input 
              placeholder="搜索姓名 (模糊)" 
              style={{ width: 180 }} 
              value={searchName}
              onChange={e => setSearchName(e.target.value)}
              onPressEnter={fetchStudents}
            />
          </Col>
          <Col>
            <Button type="primary" icon={<SearchOutlined />} onClick={fetchStudents}>
              查询
            </Button>
          </Col>
          <Col>
            <Button icon={<ReloadOutlined />} onClick={() => {
              setSearchId(undefined);
              setSearchName('');
              // 这里可以立刻重新获取全部，或者只清空条件
              // 建议：为了交互连贯性，重置后立刻刷新列表
              // 但由于 setState 是异步的，我们直接传空参数调用 fetch
              // setStudents([]); // 可选：清空当前列表让 loading 效果更明显
              // fetchStudents(); // 这里会用到闭包里的旧 state，所以下面的做法更稳妥：
              // 实际上简单的做法是：
              window.location.reload(); // 或者单独写个 reset 函数
            }}>
              重置
            </Button>
          </Col>
          <Col style={{ marginLeft: 'auto' }}>
            <Button type="primary" icon={<PlusOutlined />} onClick={openAddModal}>
              添加学生
            </Button>
          </Col>
        </Row>
      </Card>

      <Table 
        dataSource={students} 
        columns={columns as ColumnType<StudentResp>[]} 
        rowKey="stu_id" 
        loading={loading} 
        pagination={{ pageSize: 10 }}
      />
      
      <Modal 
        title={isEditMode ? "编辑学生信息" : "添加学生"} 
        open={isModalOpen} 
        onOk={handleOk} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical">
          {!isEditMode && (
            <Form.Item name="year" label="入学年份" rules={[{ required: true }]}>
              <InputNumber style={{ width: '100%' }} min={2000} max={2099} />
            </Form.Item>
          )}
          <Form.Item name="name" label="姓名" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="sex" label="性别" rules={[{ required: true }]}>
            <Select>
              <Select.Option value="M">男</Select.Option>
              <Select.Option value="F">女</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item name="age" label="年龄" rules={[{ required: true }]}>
            <InputNumber min={10} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="current_campus" label="校区" rules={[{ required: true }]}>
            <Select>
              <Select.Option value="A">A校区</Select.Option>
              <Select.Option value="B">B校区</Select.Option>
              <Select.Option value="C">C校区</Select.Option>
            </Select>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};
