
public class GetData {
	/**private String _index;
	private String _type;
	private String hostname;
	private String timestamp;
	private String log_msg;*/
	private String name;
	private String work;
	private String company;
	public GetData(String name,String work,String company){
		/*this._index=_index;
		this._type=_type;
		this.hostname=hostname;
		this.timestamp=timestamp;
		this.log_msg=log_msg;*/
		this.name=name;
		this.work=work;
		this.company=company;
		
	}
	public String getName() {
		
		return name;
		
	}
	public String getWork() {
		return work;
		
	}
	public String geCompany() {
		
		return company;
	}
	
	

}
