package maming.netcat;

/**
 * 1.�����ڲ����ǿ���ʹ���ⲿ������Ե�
 * 2.����ڲ�����static��̬��,���ǲ���ʹ���ⲿ������Ե�,�����Ҫ�ⲿ�ഫ����Ϣ����̬AcceptHandler��
 */
public class TestInnerClass {

	private String hostName;
	private int port;

	public TestInnerClass() {
		this.hostName = "http://www.baidu.com";
		this.port = 999;
	
		/*
		 * AcceptHandler acceptRunnable = new AcceptHandler();
		 * acceptRunnable.run();
		 */
	
		AcceptHandler acceptRunnable = new AcceptHandler();
		acceptRunnable.hostName = hostName;
		acceptRunnable.port = port;
		acceptRunnable.run();
	}

	private static class AcceptHandler {

		private String hostName;
		private int port;

		public void run() {
			System.out.println(toString());
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("hostss:" + hostName).append(",")
					.append("portss:" + port);
			return sb.toString();
		}

	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("host:" + hostName).append(",").append("port:" + port);
		return sb.toString();
	}

	public static void main(String[] args) {
		TestInnerClass test = new TestInnerClass();
		System.out.println(test.toString());
	}

}
