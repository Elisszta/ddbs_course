import { useEffect, useState, useMemo } from 'react';
import { 
  Table, Button, Modal, Form, Input, Select, message, 
  Popconfirm, InputNumber, Space, Card, Row, Col, Tag, Drawer, Spin 
} from 'antd';
import { 
  DeleteOutlined, EditOutlined, PlusOutlined, 
  SearchOutlined, ReloadOutlined, TeamOutlined, UserAddOutlined 
} from '@ant-design/icons';

// 引入生成的 Service 和类型
import { CourseApiService, TeacherService } from '../../client';
import type { 
  CourseResp, 
  CourseCreateParams, 
  CourseUpdateParams,
  StudentResp
} from '../../client';

const { Option } = Select;

// 简单的防抖函数实现，避免依赖 lodash
function debounce<T extends (...args: any[]) => any>(func: T, wait: number) {
  let timeout: ReturnType<typeof setTimeout> | null = null;
  return function(...args: Parameters<T>) {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(() => {
      func(...args);
    }, wait);
  };
}

export const CourseManager = () => {
  // === 状态定义 ===
  const [courses, setCourses] = useState<CourseResp[]>([]);
  const [loading, setLoading] = useState(false);
  
  // 搜索条件 - 修改默认值为 ['A', 'B', 'C']
  const [searchCampus, setSearchCampus] = useState<('A'|'B'|'C')[]>(['A', 'B', 'C']); 
  const [searchKeyword, setSearchKeyword] = useState<string>('');

  // 增改弹窗状态
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isEditMode, setIsEditMode] = useState(false);
  const [currentId, setCurrentId] = useState<number | null>(null);
  
  // 教师搜索相关 (用于下拉框)
  const [teacherOptions, setTeacherOptions] = useState<{label: string, value: number}[]>([]);
  const [fetchingTeachers, setFetchingTeachers] = useState(false);

  // 学生名单抽屉状态
  const [drawerVisible, setDrawerVisible] = useState(false);
  const [currentCourseStudents, setCurrentCourseStudents] = useState<StudentResp[]>([]);
  const [drawerLoading, setDrawerLoading] = useState(false);
  // 录当前正在管理名单的课程ID
  const [managingCourseId, setManagingCourseId] = useState<number | null>(null);
  // 待添加的学生ID输入
  const [addStudentId, setAddStudentId] = useState<number | null>(null);

  const [form] = Form.useForm();

  // === 1. 获取课程列表 ===
  const fetchCourses = async () => {
    if (searchCampus.length === 0) {
      message.warning('请至少选择一个校区');
      return;
    }
    setLoading(true);
    try {
      const res = await CourseApiService.queryCoursesApiV1CoursesGet(
        searchCampus, // campus (必填, array)
        searchKeyword || undefined, // course (关键词)
        undefined, // teacher
        undefined, // only_not_full
        undefined  // only_selected
      );
      setCourses(res.result);
    } catch (error) {
      message.error('获取课程失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchCourses(); }, []);

  // === 2. 搜索教师 (用于Select下拉) ===
  const fetchTeacherOptions = async (name: string) => {
    if (!name) return;
    setFetchingTeachers(true);
    try {
      const res = await TeacherService.searchTeacherApiV1TeachersGet(undefined, name);
      const options = res.result.map(t => ({
        label: `${t.name} (${t.teacher_id})`,
        value: t.teacher_id
      }));
      setTeacherOptions(options);
    } finally {
      setFetchingTeachers(false);
    }
  };

  // 防抖搜索
  const debouncedSearchTeachers = useMemo(() => debounce(fetchTeacherOptions, 500), []);

  // === 3. 查看课程学生 (打开抽屉) ===
  const handleViewStudents = async (courseId: number) => {
    setManagingCourseId(courseId); // 记录当前操作的课程
    setDrawerVisible(true);
    setDrawerLoading(true);
    try {
      const res = await CourseApiService.getCourseStudentsApiV1CoursesCourseIdStudentsGet(courseId);
      setCurrentCourseStudents(res.result);
    } catch (error) {
      message.error('获取学生名单失败');
    } finally {
      setDrawerLoading(false);
    }
  };

  // === 管理员添加学生进课程 ===
  const handleAddStudent = async () => {
    if (!managingCourseId || !addStudentId) {
      message.warning('请输入学生学号');
      return;
    }
    setDrawerLoading(true);
    try {
      // 调用 Select 接口 (选课)
      await CourseApiService.selectCourseApiV1CoursesCourseIdSelectPost(managingCourseId, addStudentId);
      message.success('添加学生成功');
      setAddStudentId(null); // 清空输入
      
      // 刷新学生名单
      const res = await CourseApiService.getCourseStudentsApiV1CoursesCourseIdStudentsGet(managingCourseId);
      setCurrentCourseStudents(res.result);
      
      // 刷新外层课程列表 (更新容量显示)
      fetchCourses();
    } catch (error: any) {
      // 提取错误信息
      const errorMsg = error.body?.detail?.msg || error.body?.detail || '添加失败';
      message.error(errorMsg);
    } finally {
      setDrawerLoading(false);
    }
  };

  // === 管理员移除课程中的学生 ===
  const handleRemoveStudent = async (stuId: number) => {
    if (!managingCourseId) return;
    setDrawerLoading(true);
    try {
      // 调用 Deselect 接口 (退课)
      await CourseApiService.deselectCourseApiV1CoursesCourseIdDeselectPost(managingCourseId, stuId);
      message.success('移除学生成功');
      
      // 刷新学生名单
      const res = await CourseApiService.getCourseStudentsApiV1CoursesCourseIdStudentsGet(managingCourseId);
      setCurrentCourseStudents(res.result);
      
      // 刷新外层课程列表
      fetchCourses();
    } catch (error: any) {
      const errorMsg = error.body?.detail?.msg || error.body?.detail || '移除失败';
      message.error(errorMsg);
    } finally {
      setDrawerLoading(false);
    }
  };

  // === 4. 增删改逻辑 ===
  const handleDelete = async (id: number) => {
    try {
      await CourseApiService.deleteCourseApiV1CoursesCourseIdDelete(id);
      message.success('删除成功');
      fetchCourses();
    } catch (error: any) {
      message.error(error.body?.detail || '删除失败');
    }
  };

  const handleOk = async () => {
    try {
      const values = await form.validateFields();
      
      if (isEditMode && currentId) {
        // 修改
        const updateData: CourseUpdateParams = {
          name: values.name,
          capacity: values.capacity,
          teacher_ids: values.teacher_ids
        };
        await CourseApiService.updateCourseApiV1CoursesCourseIdPut(currentId, updateData);
        message.success('修改成功');
      } else {
        // 新增
        const createData: CourseCreateParams = {
          name: values.name,
          capacity: values.capacity,
          teacher_ids: values.teacher_ids,
          campus: values.campus
        };
        await CourseApiService.createCourseApiV1CoursesPost(createData);
        message.success('创建成功');
      }
      
      setIsModalOpen(false);
      form.resetFields();
      fetchCourses();
    } catch (error: any) {
      console.error(error);
      message.error(error.body?.detail || '操作失败');
    }
  };

  const openAddModal = () => {
    setIsEditMode(false);
    setCurrentId(null);
    setTeacherOptions([]); // 重置选项
    form.resetFields();
    setIsModalOpen(true);
  };

  const openEditModal = (record: CourseResp) => {
    setIsEditMode(true);
    setCurrentId(record.course_id);
    setTeacherOptions([]); 
    
    form.setFieldsValue({
      name: record.name,
      capacity: record.capacity,
      campus: record.campus,
      teacher_ids: [] // 暂时置空，提示用户重新录入
    });
    message.info('编辑模式下请重新指定授课教师');
    
    setIsModalOpen(true);
  };

  // === 5. 列定义 ===
  const columns = [
    { title: 'ID', dataIndex: 'course_id', width: 100 },
    { title: '课程名称', dataIndex: 'name' },
    { 
      title: '校区', 
      dataIndex: 'campus', 
      width: 80,
      render: (text: string) => {
        const colors = { A: 'blue', B: 'green', C: 'orange' };
        return <Tag color={colors[text as keyof typeof colors]}>{text}</Tag>;
      }
    },
    { title: '容量', width: 120, render: (r: CourseResp) => `${r.num_selected} / ${r.capacity}` },
    { title: '授课教师', dataIndex: 'teachers', ellipsis: true },
    {
      title: '',
      width: 220,
      align: 'right' as const,
      render: (_: any, record: CourseResp) => (
        <Space>
          <Button icon={<TeamOutlined />} onClick={() => handleViewStudents(record.course_id)}>
            名单
          </Button>
          <Button icon={<EditOutlined />} onClick={() => openEditModal(record)}>
            编辑
          </Button>
          <Popconfirm title="确认删除?" onConfirm={() => handleDelete(record.course_id)}>
            <Button danger icon={<DeleteOutlined />} />
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
            <Select
              mode="multiple"
              placeholder="选择校区"
              style={{ width: 180 }}
              value={searchCampus}
              onChange={setSearchCampus}
              options={[
                { label: 'A 校区', value: 'A' },
                { label: 'B 校区', value: 'B' },
                { label: 'C 校区', value: 'C' },
              ]}
            />
          </Col>
          <Col>
            <Input 
              placeholder="搜索课程名称" 
              style={{ width: 200 }}
              value={searchKeyword}
              onChange={e => setSearchKeyword(e.target.value)}
              onPressEnter={fetchCourses}
            />
          </Col>
          <Col>
            <Button type="primary" icon={<SearchOutlined />} onClick={fetchCourses}>查询</Button>
          </Col>
          <Col style={{ marginLeft: 'auto' }}>
            <Button type="primary" icon={<PlusOutlined />} onClick={openAddModal}>创建课程</Button>
          </Col>
        </Row>
      </Card>

      <Table 
        dataSource={courses} 
        columns={columns} 
        rowKey="course_id" 
        loading={loading} 
      />

      {/* 增改弹窗 */}
      <Modal
        title={isEditMode ? "编辑课程" : "创建课程"}
        open={isModalOpen}
        onOk={handleOk}
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="课程名称" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          
          <Form.Item name="capacity" label="课程容量" rules={[{ required: true }]}>
            <InputNumber min={1} style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item name="campus" label="所属校区" rules={[{ required: true }]}>
            <Select disabled={isEditMode}>
              <Option value="A">A 校区</Option>
              <Option value="B">B 校区</Option>
              <Option value="C">C 校区</Option>
            </Select>
          </Form.Item>

          <Form.Item 
            name="teacher_ids" 
            label="授课教师" 
            rules={[{ required: true, message: '请至少选择一位教师' }]}
            extra="请输入姓名搜索教师"
          >
            <Select
              mode="multiple"
              filterOption={false}
              onSearch={debouncedSearchTeachers}
              notFoundContent={fetchingTeachers ? <Spin size="small" /> : null}
              options={teacherOptions}
              placeholder="搜索并选择教师..."
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* 学生名单抽屉 */}
      <Drawer
        title="选课学生名单"
        placement="right"
        onClose={() => setDrawerVisible(false)}
        open={drawerVisible}
        width={600} 
        extra={
          // 抽屉顶部的添加操作
          <Space>
            <InputNumber 
              placeholder="输入学生学号" 
              style={{ width: 160 }}
              value={addStudentId}
              onChange={setAddStudentId}
              controls={false}
            />
            <Button type="primary" icon={<UserAddOutlined />} onClick={handleAddStudent}>
              加入课程
            </Button>
          </Space>
        }
      >
        <Table
          loading={drawerLoading}
          dataSource={currentCourseStudents}
          rowKey="stu_id"
          pagination={false}
          columns={[
            { title: '学号', dataIndex: 'stu_id' },
            { title: '姓名', dataIndex: 'name' },
            { title: '性别', dataIndex: 'sex', width: 60 },
            { title: '校区', dataIndex: 'current_campus', width: 80 },
            {
              title: '操作',
              align: 'right',
              render: (_, record) => (
                <Popconfirm 
                  title="确定将该学生移出课程？" 
                  onConfirm={() => handleRemoveStudent(record.stu_id)}
                  okText="移出"
                  cancelText="取消"
                >
                  <Button danger size="small" type="link">移出</Button>
                </Popconfirm>
              )
            }
          ]}
        />
      </Drawer>
    </div>
  );
};