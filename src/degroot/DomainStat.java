package degroot;

public class DomainStat {
	Integer totalRequests; 
	Double averageRequestsPerHour;
	Integer maxRequestsPerHour;
	Integer totalRecords;
	
	public DomainStat(Integer totalRequests, Double averageRequestsPerHour,
			Integer maxRequestsPerHour, Integer totalRecords) {
		super();
		this.totalRequests = totalRequests;
		this.averageRequestsPerHour = averageRequestsPerHour;
		this.maxRequestsPerHour = maxRequestsPerHour;
		this.totalRecords = totalRecords;
	}
	
	public DomainStat(Integer totalRequests) {
		super();
		this.totalRequests = totalRequests;
		averageRequestsPerHour = (double)totalRequests;
		maxRequestsPerHour = totalRequests;
		totalRecords = 1;
	}

	public void update(int hourlyRequests) {
		this.totalRequests += hourlyRequests;
		if (maxRequestsPerHour < hourlyRequests) {
			maxRequestsPerHour = hourlyRequests;
		}
		
		averageRequestsPerHour = (averageRequestsPerHour * totalRecords + hourlyRequests) / (totalRecords + 1);
		totalRecords++;
	}
}
