package com.nagp.big.data.model;

import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table("invoices")
public class Invoice {

    @PrimaryKey
    private String invoiceno;
    private String pono;
    private String invoicevendorname;
    private String invoicedate;
    private String invoicerecvddate;
    private String payingorg;
    private String invoicestatusdesc;
    private String approveddate;
    private Double invoicetotal;
    private Double paidamt;
    private String invoicematchstatusdesc;
    private String createdate;
    private String org;
    private String distributionprofile;
    private String projectdesc;
    private String projectcode;
    private String fullapliabilityglacct;
}
